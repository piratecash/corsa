package dmcontrol

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Package dmcontrol is the sealed payload of the dm_control datagram: what one
// side asks the other to change about a conversation, and nothing about how it
// travels. The transport is in internal/core/node; the model is in
// docs/refactoring/reactions-protocol.md.
//
// Everything here is pure. A payload can be built, padded, sealed, opened and
// read without a node, a session or a database, which is what makes the wire
// rules testable at all.

// PayloadBucketBytes is the size EVERY plaintext payload is padded to before
// sealing.
//
// One bucket, not a ladder of them. A ladder still sorts traffic into small,
// medium and large, and the three classes line up with "acknowledged",
// "one reaction" and "a batch" closely enough to be read off the wire. One size
// leaks nothing but the frame count, and the frame count is already visible.
//
// The value has to hold a useful batch and still fit the control-class ceiling
// once sealed (domain.DatagramControlPayloadCap, 4 KiB) — see the size test.
const PayloadBucketBytes = 1024

// padFiller is what the padding is made of. A plain ASCII letter, because JSON
// never escapes it: one filler byte is always one byte on the wire, whereas an
// escaped character would make the padded length depend on the padding itself.
const padFiller = "a"

// Fact is one reaction decision on the wire.
//
// The actor is NOT here. It is taken from the frame's signed source, so there
// is no field in which to claim someone else's name — a payload cannot say
// anything about an identity that did not sign it.
//
// Field names are short because every byte of the payload is paid for twice: in
// the bucket it has to fit, and in the batch it displaces.
type Fact struct {
	MessageID domain.MessageID     `json:"m"`
	Emoji     string               `json:"e"`
	Op        domain.ReactionOp    `json:"o"`
	Clock     domain.ReactionClock `json:"c"`
}

// MaxEmojiBytes and MaxMessageIDBytes bound the two fields a peer controls.
//
// They exist because both go straight into the primary key of a table rows are
// added to but almost never removed from, so an unbounded field is the
// amplification factor of a remote memory cost. The limits are generous against
// what this build produces — a message id is a 36-byte UUID, and the longest
// emoji here is a four-codepoint sequence with modifiers — and tight against
// what a hostile peer would want to send.
const (
	MaxEmojiBytes     = 64
	MaxMessageIDBytes = 128
)

// MaxCommandNameBytes bounds the two command NAMES a peer controls: `cmd`, and
// the `refused` a refusal names.
//
// They look harmless because this build only ever writes short constants into
// them, but both end up in memory keyed by a remote peer — the refusal queue on
// the receiving side and the "this peer cannot do X" map on the sending side —
// so an unbounded name is an unbounded map with the peer holding the pen. The
// wire alphabet for a dtype is 64 bytes (domain.MaxDTypeLen) and an inner
// command has no reason to be longer.
const MaxCommandNameBytes = 64

// MaxEmojiRunes bounds how many codepoints one reaction may be.
//
// A byte cap alone is not enough: the field is DRAWN, under someone else's
// message, by a label with no line limit. Sixteen holds the longest real
// sequence this build can produce — a base emoji with a skin-tone modifier,
// a variation selector and zero-width joiners — and refuses a paragraph.
const MaxEmojiRunes = 16

// validateEmoji bounds what a peer may put under a message.
//
// The value goes into a primary key AND onto the screen, so it is checked as
// text and not only as bytes. What is refused, and why each one matters:
//
//   - invalid UTF-8, which a label renders as replacement characters and which
//     no comparison can be trusted on;
//   - control characters, newline included — a chip is one line high, and a
//     newline in it makes a chip as tall as the peer likes. U+2028 and U+2029
//     are named separately because they are line breaks that are NOT control
//     characters: unicode.IsControl covers category Cc only, and those two are
//     Zl and Zp;
//   - bidi overrides, isolates AND marks, which do not stay inside the chip:
//     they reorder the text drawn AFTER them, so one reaction can scramble the
//     conversation around it. The marks are the same class one notch weaker,
//     and are refused with the rest rather than left as the gap.
func validateEmoji(emoji string) error {
	switch {
	case emoji == "":
		return fmt.Errorf("no emoji")
	case len(emoji) > MaxEmojiBytes:
		return fmt.Errorf("a %d-byte emoji, over the %d-byte limit", len(emoji), MaxEmojiBytes)
	case !utf8.ValidString(emoji):
		return fmt.Errorf("an emoji that is not valid UTF-8")
	case utf8.RuneCountInString(emoji) > MaxEmojiRunes:
		return fmt.Errorf("an emoji of %d codepoints, over the %d-codepoint limit",
			utf8.RuneCountInString(emoji), MaxEmojiRunes)
	}
	for _, r := range emoji {
		switch {
		case unicode.IsControl(r), r == 0x2028, r == 0x2029:
			return fmt.Errorf("an emoji containing a line break or control character %U", r)
		case r >= 0x202A && r <= 0x202E, r >= 0x2066 && r <= 0x2069,
			r == 0x200E, r == 0x200F, r == 0x061C:
			return fmt.Errorf("an emoji containing a bidirectional control %U", r)
		}
	}
	return nil
}

// Validate rejects a fact that cannot mean anything, or that means more than a
// fact is allowed to cost.
//
// A zero clock is refused rather than defaulted: clocks start at 1 (see
// chatlog.NextReactionClock), so zero is either a field that never arrived or a
// sender that does not order its own decisions, and both make the merge — which
// is one comparison against the clock — silently wrong.
func (f Fact) Validate() error {
	if strings.TrimSpace(string(f.MessageID)) == "" {
		return fmt.Errorf("dmcontrol: fact without a message id")
	}
	if len(f.MessageID) > MaxMessageIDBytes {
		return fmt.Errorf("dmcontrol: fact names a %d-byte message id, over the %d-byte limit",
			len(f.MessageID), MaxMessageIDBytes)
	}
	if err := validateEmoji(f.Emoji); err != nil {
		return fmt.Errorf("dmcontrol: fact %s: %w", f.MessageID, err)
	}
	if f.Op != domain.ReactionSet && f.Op != domain.ReactionCleared {
		return fmt.Errorf("dmcontrol: fact %s carries unknown op %d", f.MessageID, f.Op)
	}
	if f.Clock == 0 {
		return fmt.Errorf("dmcontrol: fact %s carries no clock", f.MessageID)
	}
	if f.Clock > math.MaxInt64 {
		// Refused HERE and not left to the store. SQLite has no unsigned
		// integer, so such a clock cannot be written — and the store's own
		// refusal comes back as a database error, which the handler treats as a
		// transient fault and answers by releasing the replay slot. A frame that
		// can never be stored would then be re-delivered as if a retry might
		// help, and the usable facts ahead of it in the batch would be applied
		// again on every pass.
		return fmt.Errorf("dmcontrol: fact %s carries clock %d, past what a store can order",
			f.MessageID, f.Clock)
	}
	return nil
}

// Payload is the sealed plaintext of one dm_control frame.
type Payload struct {
	Version      uint32                  `json:"v"`
	Command      domain.DMControlCommand `json:"cmd"`
	Conversation domain.ConversationKind `json:"conv"`
	// Facts is set for DMControlReactions.
	Facts []Fact `json:"facts,omitempty"`
	// Refused names the command a DMControlUnsupported answer is about.
	Refused domain.DMControlCommand `json:"refused,omitempty"`
	// Pad is meaningless filler and is always present, so its own cost is
	// inside the measured length rather than a term to remember.
	Pad string `json:"pad"`
}

// ReactionsPayload is one frame's worth of reaction facts, ready to seal.
func ReactionsPayload(kind domain.ConversationKind, facts []Fact) Payload {
	return Payload{
		Version:      domain.DMControlSchemaVersion,
		Command:      domain.DMControlReactions,
		Conversation: kind,
		Facts:        facts,
	}
}

// UnsupportedPayload answers a command this build does not know.
func UnsupportedPayload(kind domain.ConversationKind, refused domain.DMControlCommand) Payload {
	return Payload{
		Version:      domain.DMControlSchemaVersion,
		Command:      domain.DMControlUnsupported,
		Conversation: kind,
		Refused:      refused,
	}
}

// Encode marshals a payload and pads it to PayloadBucketBytes.
//
// The padding is measured, not predicted: the payload is marshalled once
// without filler, and the filler is the shortfall. Predicting the length would
// mean re-deriving encoding/json's escaping rules here, and a prediction that
// is one byte out produces frames of two distinct sizes — which is the one
// thing the padding exists to prevent.
//
// A payload that does not fit is an error rather than an unpadded frame:
// callers split batches (see PackReactions), and silently emitting an
// over-sized frame would leak the batch size of exactly the busiest moments.
func Encode(payload Payload) ([]byte, error) {
	if err := payload.validateForSend(); err != nil {
		return nil, err
	}
	payload.Pad = ""
	bare, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("dmcontrol: marshal payload: %w", err)
	}
	if len(bare) > PayloadBucketBytes {
		return nil, fmt.Errorf(
			"dmcontrol: %s payload is %d bytes, over the %d-byte bucket",
			payload.Command, len(bare), PayloadBucketBytes)
	}
	payload.Pad = strings.Repeat(padFiller, PayloadBucketBytes-len(bare))
	padded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("dmcontrol: marshal padded payload: %w", err)
	}
	if len(padded) != PayloadBucketBytes {
		return nil, fmt.Errorf(
			"dmcontrol: padded payload is %d bytes, want %d", len(padded), PayloadBucketBytes)
	}
	return padded, nil
}

// validateForSend refuses a payload this node should never put on the wire.
func (p Payload) validateForSend() error {
	if p.Version != domain.DMControlSchemaVersion {
		return fmt.Errorf("dmcontrol: payload version %d, want %d", p.Version, domain.DMControlSchemaVersion)
	}
	switch p.Conversation {
	case domain.ConversationDirect:
	case domain.ConversationGroup:
		return fmt.Errorf("dmcontrol: group conversations are not supported yet")
	default:
		return fmt.Errorf("dmcontrol: unknown conversation kind %q", p.Conversation)
	}
	switch p.Command {
	case domain.DMControlReactions:
		if len(p.Facts) == 0 {
			return fmt.Errorf("dmcontrol: a reactions payload with no facts")
		}
		for _, fact := range p.Facts {
			if err := fact.Validate(); err != nil {
				return err
			}
		}
	case domain.DMControlUnsupported:
		if p.Refused == "" {
			return fmt.Errorf("dmcontrol: an unsupported answer that names no command")
		}
	default:
		return fmt.Errorf("dmcontrol: refusing to send unknown command %q", p.Command)
	}
	return nil
}

// Decode reads a sealed plaintext back.
//
// It checks the SHAPE and not the vocabulary: an unknown command decodes
// successfully and keeps its name, because naming it back is exactly what a
// DMControlUnsupported answer has to do. Deciding what a command means is the
// handler's job, and refusing here would leave it nothing to report.
func Decode(raw []byte) (Payload, error) {
	var payload Payload
	// Unknown fields are ignored on purpose: a later version adding a field
	// this build does not read must still be understood as far as it goes,
	// which is the whole point of versioning the layout rather than the type.
	if err := json.Unmarshal(raw, &payload); err != nil {
		return Payload{}, fmt.Errorf("dmcontrol: unmarshal payload: %w", err)
	}
	if payload.Version != domain.DMControlSchemaVersion {
		return Payload{}, fmt.Errorf(
			"dmcontrol: payload version %d, want %d", payload.Version, domain.DMControlSchemaVersion)
	}
	if err := validateCommandName("cmd", payload.Command); err != nil {
		return Payload{}, err
	}
	if payload.Refused != "" {
		if err := validateCommandName("refused", payload.Refused); err != nil {
			return Payload{}, err
		}
	}
	payload.Pad = ""
	return payload, nil
}

// validateCommandName bounds a name this build may not recognise but will
// remember. Unknown names are the point of the field, so the check is on shape
// and size only — see MaxCommandNameBytes.
func validateCommandName(field string, name domain.DMControlCommand) error {
	switch {
	case name == "":
		return fmt.Errorf("dmcontrol: payload names no %s", field)
	case len(name) > MaxCommandNameBytes:
		return fmt.Errorf("dmcontrol: %s is %d bytes, over the %d-byte limit",
			field, len(name), MaxCommandNameBytes)
	}
	for _, r := range name {
		// The same alphabet a dtype uses, for the same reason: a name that
		// reaches a log line, a map key and a metrics label must not be able to
		// carry anything but a name.
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return fmt.Errorf("dmcontrol: %s %q is not a command name", field, name)
	}
	return nil
}

// PackReactions splits facts into as few frames as fit the bucket.
//
// Greedy rather than balanced: the frames are all one size on the wire anyway,
// so a balanced split would only trade a full frame for two half-empty ones.
//
// A single fact that does not fit alone is an error and not a dropped fact: it
// can only come from an emoji or a message id far outside what Fact.Validate
// admits, and losing it silently would leave the two sides permanently apart
// with nothing to notice it.
//
// Everything that CAN travel is returned WITH the error — the facts before the
// bad one AND the facts after it. An unusable fact costs only itself: on a
// transport with no retry, stopping at it would turn one bad row into the loss
// of every fact queued behind it, and since the queue offers the same page again
// the loss would repeat for as long as the row exists.
//
// The error names the FIRST unusable fact; the count of them is in it too, so a
// log line says how much was skipped without the caller walking the list again.
//
// The fit is decided by measuring each fact ONCE and summing, not by
// re-marshalling the growing batch per fact: this runs on the flush path with
// however many facts a burst produced, and the obvious version is quadratic in
// that number.
func PackReactions(kind domain.ConversationKind, facts []Fact) ([][]byte, error) {
	if len(facts) == 0 {
		return nil, nil
	}
	room, err := factRoomPerFrame(kind)
	if err != nil {
		return nil, err
	}

	var frames [][]byte
	batch := make([]Fact, 0, len(facts))
	used := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		frame, err := Encode(ReactionsPayload(kind, batch))
		if err != nil {
			return err
		}
		frames = append(frames, frame)
		batch = batch[:0]
		used = 0
		return nil
	}

	// The first reason a fact was skipped, and how many were. Kept rather than
	// returned on the spot: the walk goes on to the facts behind it, because an
	// unusable fact must cost only itself — stopping at it would turn one bad
	// row into the loss of every fact queued behind it, and the same page is
	// offered again, so the loss would repeat for as long as the row exists.
	var cause error
	skipped := 0
	skip := func(reason error) {
		skipped++
		if cause == nil {
			cause = reason
		}
	}

	for _, fact := range facts {
		if err := fact.Validate(); err != nil {
			skip(err)
			continue
		}
		cost, err := factCost(fact)
		if err != nil {
			skip(err)
			continue
		}
		if cost > room {
			skip(fmt.Errorf(
				"dmcontrol: fact %s needs %d bytes, more than the %d a whole frame has",
				fact.MessageID, cost, room))
			continue
		}
		if used+cost > room {
			if err := flush(); err != nil {
				return frames, err
			}
		}
		batch = append(batch, fact)
		used += cost
	}
	if err := flush(); err != nil {
		return frames, err
	}
	if cause != nil {
		return frames, fmt.Errorf("dmcontrol: %d of %d facts could not be packed: %w",
			skipped, len(facts), cause)
	}
	return frames, nil
}

// factRoomPerFrame is how many bytes of fact list one frame has.
//
// Measured from a payload carrying ONE fact, with that fact's own cost taken
// back off. An empty-list payload cannot be used and using one was a real bug:
// `Facts` is `omitempty`, so an empty slice omits the key entirely and the
// measurement misses `"facts":[],` — about ten bytes. Batches whose greedy sum
// landed in those ten bytes then failed to Encode, and the whole batch was
// dropped, which on a transport with no retry means the facts are gone.
func factRoomPerFrame(kind domain.ConversationKind) (int, error) {
	// Content irrelevant: only the bytes the ENVELOPE adds around it are kept.
	probe := Fact{MessageID: "m", Emoji: "e", Op: domain.ReactionSet, Clock: 1}
	payload := ReactionsPayload(kind, []Fact{probe})
	payload.Pad = ""
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("dmcontrol: measure the frame envelope: %w", err)
	}
	probeJSON, err := json.Marshal(probe)
	if err != nil {
		return 0, fmt.Errorf("dmcontrol: measure the probe fact: %w", err)
	}
	return PayloadBucketBytes - (len(encoded) - len(probeJSON)), nil
}

// factCost is what one fact adds to the list: its own JSON plus the comma that
// joins it to the previous one. Counting the comma for the first fact too is a
// one-byte overestimate per frame, which is the safe direction.
func factCost(fact Fact) (int, error) {
	encoded, err := json.Marshal(fact)
	if err != nil {
		return 0, fmt.Errorf("dmcontrol: measure fact %s: %w", fact.MessageID, err)
	}
	return len(encoded) + 1, nil
}
