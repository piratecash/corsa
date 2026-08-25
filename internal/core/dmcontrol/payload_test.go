package dmcontrol

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func setFact(id, emoji string, clock uint64) Fact {
	return Fact{
		MessageID: domain.MessageID(id),
		Emoji:     emoji,
		Op:        domain.ReactionSet,
		Clock:     domain.ReactionClock(clock),
	}
}

// Every frame this node emits is the same size, whatever it says. A relay that
// could tell a refusal from a reaction, or one reaction from twelve, would be
// reading the feature off the wire without opening anything.
func TestEveryPayloadIsOneSize(t *testing.T) {
	cases := map[string]Payload{
		"one fact":  ReactionsPayload(domain.ConversationDirect, []Fact{setFact("m1", "👍", 1)}),
		"ten facts": ReactionsPayload(domain.ConversationDirect, manyFacts(10)),
		"refusal":   UnsupportedPayload(domain.ConversationDirect, "message_delete"),
	}
	for name, payload := range cases {
		encoded, err := Encode(payload)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		if len(encoded) != PayloadBucketBytes {
			t.Fatalf("%s encoded to %d bytes, want the %d-byte bucket", name, len(encoded), PayloadBucketBytes)
		}
	}
}

// The bucket is only a constant frame size if the sealed frame also fits the
// class it travels in. If it did not, the sender would have to shrink some
// frames and the padding would have bought nothing.
func TestASealedBucketFitsTheControlClass(t *testing.T) {
	sealed := PayloadBucketBytes + SealOverheadBytes
	cap, err := domain.DatagramPayloadCap(domain.DatagramClassControl)
	if err != nil {
		t.Fatalf("control payload cap: %v", err)
	}
	if sealed > cap {
		t.Fatalf("a sealed bucket is %d bytes, over the %d-byte control cap", sealed, cap)
	}
}

// Padding is measured against the real marshalled bytes, so a payload full of
// characters JSON escapes must still land on the bucket exactly. The escapes
// live in the message id: the emoji is checked as text and refuses them.
func TestPaddingSurvivesEscapedContent(t *testing.T) {
	encoded, err := Encode(ReactionsPayload(domain.ConversationDirect, []Fact{
		setFact(`m"1\`+"\n", "👍", 7),
	}))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(encoded) != PayloadBucketBytes {
		t.Fatalf("escaped content encoded to %d bytes, want %d", len(encoded), PayloadBucketBytes)
	}
}

func TestRoundTripKeepsTheFacts(t *testing.T) {
	facts := []Fact{
		setFact("m1", "👍", 4),
		{MessageID: "m2", Emoji: "🔥", Op: domain.ReactionCleared, Clock: 5},
	}
	encoded, err := Encode(ReactionsPayload(domain.ConversationDirect, facts))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Command != domain.DMControlReactions || decoded.Conversation != domain.ConversationDirect {
		t.Fatalf("decoded as %q/%q", decoded.Command, decoded.Conversation)
	}
	if decoded.Pad != "" {
		t.Fatal("the filler survived the decode: it would reach the caller as content")
	}
	if len(decoded.Facts) != len(facts) {
		t.Fatalf("decoded %d facts, sent %d", len(decoded.Facts), len(facts))
	}
	for i, fact := range decoded.Facts {
		if fact != facts[i] {
			t.Fatalf("fact %d came back as %#v, sent %#v", i, fact, facts[i])
		}
	}
}

// A command this build has never heard of has to survive decoding with its
// name intact — naming it back is the whole content of the refusal, and
// rejecting it here would leave the handler nothing to report.
func TestDecodeKeepsAnUnknownCommandsName(t *testing.T) {
	raw := []byte(`{"v":1,"cmd":"message_edit","conv":"direct","pad":"aaa"}`)
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("an unknown command failed to decode: %v", err)
	}
	if decoded.Command != "message_edit" {
		t.Fatalf("command came back as %q", decoded.Command)
	}
}

// A version this build cannot read is refused whole rather than read as far as
// it parses: a layout change is exactly the case where the fields that do
// unmarshal may no longer mean what they used to.
func TestDecodeRefusesAnotherLayoutVersion(t *testing.T) {
	if _, err := Decode([]byte(`{"v":9,"cmd":"reactions","conv":"direct"}`)); err == nil {
		t.Fatal("a payload from another layout version was accepted")
	}
}

// Refused on the way out, so a malformed fact cannot leave this node and become
// the peer's problem to reconcile.
func TestEncodeRefusesWhatCannotBeMerged(t *testing.T) {
	cases := map[string]Payload{
		"no clock":    ReactionsPayload(domain.ConversationDirect, []Fact{{MessageID: "m1", Emoji: "👍"}}),
		"no emoji":    ReactionsPayload(domain.ConversationDirect, []Fact{{MessageID: "m1", Clock: 1}}),
		"no message":  ReactionsPayload(domain.ConversationDirect, []Fact{{Emoji: "👍", Clock: 1}}),
		"no facts":    ReactionsPayload(domain.ConversationDirect, nil),
		"no refusal":  {Version: domain.DMControlSchemaVersion, Command: domain.DMControlUnsupported, Conversation: domain.ConversationDirect},
		"a group":     ReactionsPayload(domain.ConversationGroup, []Fact{setFact("m1", "👍", 1)}),
		"unknown cmd": {Version: domain.DMControlSchemaVersion, Command: "message_edit", Conversation: domain.ConversationDirect},
	}
	for name, payload := range cases {
		if _, err := Encode(payload); err == nil {
			t.Fatalf("%s was accepted for sending", name)
		}
	}
}

// A batch larger than one frame becomes several full frames rather than one
// over-sized one: the frame count is already visible, the frame size is not.
func TestPackReactionsFillsWholeFrames(t *testing.T) {
	facts := manyFacts(200)
	frames, err := PackReactions(domain.ConversationDirect, facts)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if len(frames) < 2 {
		t.Fatalf("200 facts packed into %d frame(s); the batch was meant to overflow", len(frames))
	}
	seen := 0
	for i, frame := range frames {
		if len(frame) != PayloadBucketBytes {
			t.Fatalf("frame %d is %d bytes, want %d", i, len(frame), PayloadBucketBytes)
		}
		decoded, err := Decode(frame)
		if err != nil {
			t.Fatalf("frame %d: decode: %v", i, err)
		}
		for j, fact := range decoded.Facts {
			if fact != facts[seen+j] {
				t.Fatalf("frame %d fact %d is %#v, want %#v", i, j, fact, facts[seen+j])
			}
		}
		seen += len(decoded.Facts)
	}
	if seen != len(facts) {
		t.Fatalf("packing carried %d of %d facts", seen, len(facts))
	}
}

// A fact that cannot travel is an error, not a dropped fact: dropping it would
// leave the two sides permanently apart with nothing to notice it.
//
// Today the field limits make "too large for a frame" unreachable, and the
// second half of this test is what keeps that true — the two bounds are set
// independently of the bucket and could drift into contradiction.
func TestPackReactionsRefusesAFactThatCannotTravel(t *testing.T) {
	if _, err := PackReactions(domain.ConversationDirect, []Fact{
		setFact("m1", strings.Repeat("a", PayloadBucketBytes), 1),
	}); err == nil {
		t.Fatal("a fact too large for any frame was packed anyway")
	}

	room, err := factRoomPerFrame(domain.ConversationDirect)
	if err != nil {
		t.Fatalf("measure the frame: %v", err)
	}
	cost, err := factCost(setFact(
		strings.Repeat("m", MaxMessageIDBytes), strings.Repeat("a", MaxEmojiBytes), 1<<62))
	if err != nil {
		t.Fatalf("measure the largest admissible fact: %v", err)
	}
	if cost > room {
		t.Fatalf("the largest fact the limits admit costs %d bytes but a frame has %d", cost, room)
	}
}

// Both fields a peer controls end up in the primary key of a table rows are
// added to and almost never removed from, so an unbounded one is the
// amplification factor of a remote memory cost.
func TestAFactCannotCarryUnboundedFields(t *testing.T) {
	oversized := map[string]Fact{
		"emoji in bytes":      setFact("m1", strings.Repeat("👍", MaxEmojiBytes), 1),
		"emoji in codepoints": setFact("m1", strings.Repeat("a", MaxEmojiRunes+1), 1),
		"message id":          setFact(strings.Repeat("m", MaxMessageIDBytes+1), "👍", 1),
	}
	for name, fact := range oversized {
		if err := fact.Validate(); err == nil {
			t.Fatalf("an oversized %s was accepted", name)
		}
	}
	atTheLimit := []Fact{
		// The byte limit is what a codepoint-heavy sequence hits first, so the
		// two are exercised by one fact at each ceiling.
		setFact("m1", strings.Repeat("👍", MaxEmojiRunes), 1),
		setFact(strings.Repeat("m", MaxMessageIDBytes), "👍", 2),
	}
	for _, fact := range atTheLimit {
		if err := fact.Validate(); err != nil {
			t.Fatalf("a fact exactly at the limit was refused: %v", err)
		}
	}
	// And a frame of the largest facts the limits admit still packs, so the
	// two bounds cannot combine into a fact nothing can carry.
	if _, err := PackReactions(domain.ConversationDirect, atTheLimit); err != nil {
		t.Fatalf("facts at the field limits could not be packed: %v", err)
	}
}

func manyFacts(n int) []Fact {
	facts := make([]Fact, 0, n)
	for i := range n {
		facts = append(facts, setFact(
			"0123456789abcdef0123456789abcdef"+string(rune('a'+i%26)),
			"👍", uint64(i+1)))
	}
	return facts
}

// A command name is written by a peer and ends up as a map key and a log field
// on both sides — the refusal queue here, the "this peer cannot do X" memory
// there. So it is bounded and its alphabet is closed, while remaining a name
// this build has never heard of, which is the whole point of the field.
func TestDecodeBoundsCommandNames(t *testing.T) {
	oversized := `{"v":1,"cmd":"` + strings.Repeat("a", MaxCommandNameBytes+1) + `","conv":"direct"}`
	if _, err := Decode([]byte(oversized)); err == nil {
		t.Fatal("an oversized command name was accepted")
	}
	if _, err := Decode([]byte(`{"v":1,"cmd":"Message Edit!","conv":"direct"}`)); err == nil {
		t.Fatal("a command name outside the wire alphabet was accepted")
	}
	if _, err := Decode([]byte(`{"v":1,"cmd":"unsupported","refused":"` +
		strings.Repeat("b", MaxCommandNameBytes+1) + `","conv":"direct"}`)); err == nil {
		t.Fatal("an oversized refused-command name was accepted")
	}
	// A name at the limit, in the alphabet, and unknown to this build still
	// decodes: refusing it would leave the handler nothing to answer with.
	atTheLimit := `{"v":1,"cmd":"` + strings.Repeat("a", MaxCommandNameBytes) + `","conv":"direct"}`
	if _, err := Decode([]byte(atTheLimit)); err != nil {
		t.Fatalf("a name exactly at the limit was refused: %v", err)
	}
}

// The emoji is written by a peer and DRAWN under someone else's message, so it
// is checked as text and not only as bytes.
func TestAnEmojiIsCheckedAsTextNotJustBytes(t *testing.T) {
	refused := map[string]string{
		"a newline":           "👍\n👍",
		"a tab":               "👍\t",
		"a bare control byte": "\x01",
		"invalid UTF-8":       "\xff\xfe",
		"a bidi override":     "\u202e👍",
		"a bidi isolate":      "\u2066👍",
		"too many codepoints": strings.Repeat("a", MaxEmojiRunes+1),
		"nothing at all":      "",
	}
	for name, emoji := range refused {
		if err := validateEmoji(emoji); err == nil {
			t.Fatalf("%s was accepted as an emoji", name)
		}
	}

	// What real reactions look like, including the sequences a byte or rune
	// cap alone would have caught: a skin tone, a variation selector, a ZWJ
	// family. None of these may be refused.
	accepted := []string{"👍", "❤️", "👍🏽", "👨‍👩‍👧‍👦", "\U0001F3F3\ufe0f\u200d\U0001F308", "!"}
	for _, emoji := range accepted {
		if err := validateEmoji(emoji); err != nil {
			t.Fatalf("%q was refused: %v", emoji, err)
		}
	}
}

// The greedy fit has to hold for facts of MIXED size, which is what a real
// batch is: emoji are one to eleven bytes and ids vary.
//
// A homogeneous batch cannot catch a room measured a few bytes too large — the
// running total steps by a constant and skips the window where the error shows.
// This walks the offset so some trial lands in it. The bug it pins was exactly
// that: `factRoomPerFrame` measured an EMPTY-list payload, and `Facts` is
// omitempty, so the measurement missed `"facts":[],` and a batch that overshot
// failed to encode — losing every fact in it, on a transport with no retry.
func TestPackReactionsFitsBatchesOfMixedSizes(t *testing.T) {
	emoji := []string{"👍", "❤️", "🔥", "😂", "😮", "😢", "🙏", "a", "👨‍👩‍👧‍👦"}
	for offset := range 64 {
		facts := make([]Fact, 0, 80)
		for i := range 80 {
			facts = append(facts, Fact{
				MessageID: domain.MessageID(fmt.Sprintf("%036d", i+offset)),
				Emoji:     emoji[(i+offset)%len(emoji)],
				Op:        domain.ReactionSet,
				Clock:     domain.ReactionClock(i + 1),
			})
		}
		frames, err := PackReactions(domain.ConversationDirect, facts)
		if err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
		carried := 0
		for i, frame := range frames {
			if len(frame) != PayloadBucketBytes {
				t.Fatalf("offset %d frame %d is %d bytes, want %d",
					offset, i, len(frame), PayloadBucketBytes)
			}
			decoded, err := Decode(frame)
			if err != nil {
				t.Fatalf("offset %d frame %d: decode: %v", offset, i, err)
			}
			carried += len(decoded.Facts)
		}
		if carried != len(facts) {
			t.Fatalf("offset %d carried %d of %d facts", offset, carried, len(facts))
		}
	}
}

// A fact that cannot travel loses only itself. EVERY good fact before it comes
// back with the error, the partial frame included — on a transport with no
// retry, dropping them turns one unusable fact into the loss of everything
// queued behind it.
//
// Counting the facts and not the frames is the point: returning only the frames
// already flushed passes a frame-count assertion while silently dropping the
// batch in progress, which is a whole frame's worth short of correct.
func TestPackReactionsKeepsEveryFactItCouldCarry(t *testing.T) {
	good := manyFacts(60)
	frames, err := PackReactions(domain.ConversationDirect,
		append(append([]Fact{}, good...), Fact{MessageID: "bad", Emoji: "👍"})) // no clock
	if err == nil {
		t.Fatal("an unmergeable fact was packed")
	}
	carried := 0
	for i, frame := range frames {
		if len(frame) != PayloadBucketBytes {
			t.Fatalf("frame %d is %d bytes, want %d", i, len(frame), PayloadBucketBytes)
		}
		decoded, decodeErr := Decode(frame)
		if decodeErr != nil {
			t.Fatalf("frame %d: decode: %v", i, decodeErr)
		}
		carried += len(decoded.Facts)
	}
	if carried != len(good) {
		t.Fatalf("carried %d of the %d good facts", carried, len(good))
	}
}

// An unusable fact costs only itself — including the facts BEHIND it. Stopping
// at it would turn one bad row into the loss of everything queued after it, and
// since the queue offers the same page again, that loss would repeat for as long
// as the row exists.
func TestPackReactionsSkipsABadFactAndKeepsTheOnesBehindIt(t *testing.T) {
	facts := []Fact{
		{MessageID: "m1", Emoji: "👍", Op: domain.ReactionSet, Clock: 1},
		// Refused by Fact.Validate, in the MIDDLE: at the end it would be
		// indistinguishable from stopping there.
		{MessageID: "m2", Emoji: "", Op: domain.ReactionSet, Clock: 2},
		{MessageID: "m3", Emoji: "🔥", Op: domain.ReactionCleared, Clock: 3},
	}

	frames, err := PackReactions(domain.ConversationDirect, facts)
	if err == nil {
		t.Fatal("the bad fact was packed without a word")
	}
	if len(frames) == 0 {
		t.Fatal("nothing was packed at all")
	}

	var carried []Fact
	for _, frame := range frames {
		payload, decodeErr := Decode(frame)
		if decodeErr != nil {
			t.Fatalf("decode: %v", decodeErr)
		}
		carried = append(carried, payload.Facts...)
	}
	if len(carried) != 2 {
		t.Fatalf("the frames carry %d facts, want the two usable ones: %#v", len(carried), carried)
	}
	if carried[0].MessageID != "m1" || carried[1].MessageID != "m3" {
		t.Fatalf("the frames carry %s and %s, want m1 and m3", carried[0].MessageID, carried[1].MessageID)
	}
}

// A clock a store cannot hold is refused at the door.
//
// SQLite has no unsigned integer, so anything past MaxInt64 cannot be written —
// and the store's refusal comes back as a database error, which the handler
// reads as a transient fault and answers by releasing the replay slot. A frame
// that can never be stored would then be re-delivered as if a retry might help.
func TestAFactRefusesAClockNoStoreCanOrder(t *testing.T) {
	fact := Fact{MessageID: "m1", Emoji: "👍", Op: domain.ReactionSet, Clock: math.MaxInt64}
	if err := fact.Validate(); err != nil {
		t.Fatalf("the largest clock a store CAN hold was refused: %v", err)
	}
	fact.Clock++
	if err := fact.Validate(); err == nil {
		t.Fatal("a clock past what a store can hold was accepted")
	}
}
