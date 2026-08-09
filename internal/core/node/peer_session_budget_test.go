package node

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// peer_session_budget_test.go covers the RESPONSE-plane admission of the
// outbound peer-session reader: the per-neighbour raw budget applied around the
// read, the entitlement gates on the two wide types, and the violation ledger
// that turns a repeated violation into a teardown.
//
// The three obligations pinned here are the three holes the budget closes:
//
//   - §4.1 step 1 / §5 (spec lines 417, 853-862): the byte budget is charged
//     BEFORE the expensive work. On this reader the expensive work starts at
//     the read itself, because it accepts up to maxResponseLineBytes (8 MiB)
//     and every one of those bytes is read and copied before anything looks at
//     the frame;
//   - a size violation must cost the sender something, or it can be repeated
//     for free;
//   - the wide budget belongs to a REPLY (`contacts`) and to the one
//     unsolicited type (`push_message`), and each needs its own bound: the
//     reply needs an outstanding request, the unsolicited type needs a budget
//     of its own, because the 256-slot session inbox is where multi-megabyte
//     frames stop being CPU and become resident memory.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// countingReader reports how many bytes were actually pulled from the wire, so
// a test can assert on what the reader READ rather than on what it returned.
type countingReader struct {
	src  io.Reader
	read int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.src.Read(p)
	c.read += n
	return n, err
}

// oversizeLineOfType builds a well-formed single-type line past MaxFrameLine.
func oversizeLineOfType(t *testing.T, frameType string, padBytes int) string {
	t.Helper()
	line := `{"type":"` + frameType + `","pad":"` + strings.Repeat("p", padBytes) + `"}` + "\n"
	if wireLineBudget(line) <= protocol.MaxFrameLine {
		t.Fatalf("fixture is %d bytes, it must exceed MaxFrameLine", wireLineBudget(line))
	}
	return line
}

// freezeSessionAdmission stops the session's budget clock and returns the
// instant it is stopped at, so a test can spend a bucket without the refill
// handing it straight back.
func freezeSessionAdmission(t *testing.T, session *peerSession) {
	t.Helper()
	frozen := time.Now()
	session.admission.setClock(func() time.Time { return frozen })
	// One charge of nothing initialises the buckets at the frozen instant; the
	// zero value fills them lazily, and a test that wrote to them first would
	// have its writes overwritten by that first fill.
	session.admission.chargeLine(0, false)
}

// sessionRawBytesRemaining reads the shared byte bucket.
func sessionRawBytesRemaining(session *peerSession) float64 {
	session.admission.mu.Lock()
	defer session.admission.mu.Unlock()
	return session.admission.bytes
}

// sessionViolations reads the violation ledger.
func sessionViolations(session *peerSession) float64 {
	session.admission.mu.Lock()
	defer session.admission.mu.Unlock()
	return session.admission.violations
}

// drainSessionPushBudget empties the unsolicited-push sub-budget and leaves the
// shared budget untouched, which is what makes an assertion about the two
// budgets discriminating rather than about "some budget ran out".
func drainSessionPushBudget(session *peerSession) {
	session.admission.mu.Lock()
	defer session.admission.mu.Unlock()
	session.admission.pushBytes = 0
}

// setSessionRawBudget puts the SHARED byte bucket at a chosen level, which is
// how a test reaches the state a neighbour reaches by sending anything at all
// faster than the budget refills — a file transfer, a DM backlog or a
// connect-time full sync will do it, and the bucket is shared, so what spends it
// need not be what meets it empty.
//
// It is paired with freezeSessionAdmission: the bucket refills in continuous
// time, so without a stopped clock the level is restored before the test's first
// line arrives.
func setSessionRawBudget(session *peerSession, bytes float64) {
	session.admission.mu.Lock()
	defer session.admission.mu.Unlock()
	session.admission.bytes = bytes
}

// drainSessionRawBudget empties the shared byte bucket.
func drainSessionRawBudget(session *peerSession) {
	setSessionRawBudget(session, 0)
}

// sameSizeContactsLine returns an unsolicited `contacts` line of EXACTLY the
// length of the datagram line it is measured against.
//
// It is the non-datagram half of every comparison in this file: the same number
// of bytes, from the same neighbour, on the same reader, differing only in the
// type. `contacts` is chosen because an unsolicited one is charged and then
// dropped by refuseUnsolicitedReplyLine without a violation of its own
// (peer_session_admission.go) — so it moves the shared bucket and nothing else,
// which is what a control has to do to stay a control.
func sameSizeContactsLine(t *testing.T, size int) string {
	t.Helper()
	const envelope = `{"type":"contacts","pad":""}` + "\n"
	if size <= len(envelope) {
		t.Fatalf("a %d-byte control line cannot carry the %d-byte envelope", size, len(envelope))
	}
	line := `{"type":"contacts","pad":"` + strings.Repeat("c", size-len(envelope)) + `"}` + "\n"
	if len(line) != size {
		t.Fatalf("control line is %d bytes, want %d", len(line), size)
	}
	if wireLineBudget(line) > protocol.MaxFrameLine {
		t.Fatal("the control line must stay inside the strict budget, or it is refused during the read instead of being charged")
	}
	return line
}

// ---------------------------------------------------------------------------
// The raw budget is applied around the read, not after it
// ---------------------------------------------------------------------------

// TestStagedReadStopsAtTheSoftLimitWhenTheExtensionIsRefused is the ordering
// test for finding 2's first hole, stated where the hole is: in the READ.
//
// Before the staging, readPeerSession called readFrameLine with a flat
// maxResponseLineBytes, so a line was read and copied in full — up to eight
// megabytes — and only then classified. "Admission before any decoding" held
// against the parser and against nothing else.
//
// The assertion is on the COUNTING READER: a refused line must leave the socket
// having given up at most the soft limit plus the one buffer fill that crossed
// it. The mutation this kills is the original code — hand the reader
// maxResponseLineBytes and judge afterwards — under which the count is the
// whole line.
func TestStagedReadStopsAtTheSoftLimitWhenTheExtensionIsRefused(t *testing.T) {
	t.Parallel()

	const (
		softLimit  = 8 << 10
		bufferSize = 4 << 10
		lineBytes  = 4 << 20
	)
	source := &countingReader{src: strings.NewReader(
		`{"type":"push_message","pad":"` + strings.Repeat("p", lineBytes) + `"}` + "\n",
	)}
	reader := bufio.NewReaderSize(source, bufferSize)

	asked := ""
	read, err := readFrameLineStaged(reader, softLimit, func(prefix string) int {
		asked = prefix
		return 0
	})

	if err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("readFrameLineStaged err = %v, want the size-limit refusal", err)
	}
	if read.line != "" || read.delimited {
		t.Fatalf("a refused line must yield nothing and no delimiter; got %d bytes, delimited=%v", len(read.line), read.delimited)
	}
	if read.consumed > softLimit+bufferSize {
		t.Fatalf("the read reports %d bytes consumed for a refused line; it cannot exceed the limit by more than the chunk that crossed it (%d)",
			read.consumed, softLimit+bufferSize)
	}
	if claimed, named := claimedFrameTypeFromPrefix(asked); !named || claimed != "push_message" {
		t.Fatalf("the gate was asked about %q (named=%v); it must see the claim at the head of the line", claimed, named)
	}
	if source.read > softLimit+bufferSize {
		t.Fatalf("the reader pulled %d bytes off the wire for a refused line; the budget must stop it within %d",
			source.read, softLimit+bufferSize)
	}
}

// readAdmittedSessionLineBytesRead drives the PRODUCTION read entry point over
// a counting reader and reports how many bytes it pulled off the wire for one
// line, plus the verdict.
//
// It exists because the property under test is invisible from the session
// inbox: a line refused after being read and a line refused before being read
// look identical there, and only the second one is the finding.
func readAdmittedSessionLineBytesRead(t *testing.T, wire, expect string) (int, error) {
	t.Helper()

	svc := &Service{}
	session := &peerSession{address: "10.9.9.9:64646"}
	if expect != "" {
		defer session.admission.expectReply(expect)()
	}
	source := &countingReader{src: strings.NewReader(wire)}
	_, err := svc.readAdmittedSessionLine(bufio.NewReader(source), session)
	return source.read, err
}

// TestReadAdmittedSessionLineStopsAtTheStrictBudget is the WIRING half of the
// staging: the production reader has to hand readFrameLineStaged
// protocol.MaxFrameLine as its soft limit, not maxResponseLineBytes.
//
// The mutation this kills is the code as it stood — one flat 8 MiB limit — and
// it is invisible to every inbox-level test, because a line read in full and
// then refused reaches the dispatcher exactly as seldom as one refused before
// the read. The only difference is the eight megabytes, so the eight megabytes
// is what the assertion is about.
func TestReadAdmittedSessionLineStopsAtTheStrictBudget(t *testing.T) {
	t.Parallel()

	// A `contacts` line with no request outstanding: entitled by type, refused
	// by entitlement — the exact case that used to buy 8 MiB of read and copy.
	wire := oversizeLineOfType(t, "contacts", 4<<20)

	read, err := readAdmittedSessionLineBytesRead(t, wire, "")
	if !errors.Is(err, errPeerSessionLineRefused) {
		t.Fatalf("readAdmittedSessionLine err = %v, want the refusal sentinel", err)
	}
	if read < protocol.MaxFrameLine {
		t.Fatalf("the reader gave up after %d bytes: it must read up to the strict budget before judging", read)
	}
	// The reader consumes the refused remainder so it cannot be read as frames
	// of its own — the DISCARD allocates nothing and copies nothing. What must
	// never happen is the line being MATERIALISED, which is what the companion
	// test below pins on the returned value.
	if read != len(wire) {
		t.Fatalf("the refused line left %d of %d bytes in the stream to be read as frames of their own",
			len(wire)-read, len(wire))
	}
}

// TestSolicitedWideLineIsReadWholeAndUnsolicitedIsNot is the discriminating
// pair for the entitlement gate, taken at the read rather than at the inbox.
//
// Both lines are byte-identical; only the outstanding request differs. With the
// request the line is materialised whole — that is the legitimate batched reply
// the wide budget exists for. Without it the line is refused and NOTHING of it
// is built.
//
// The mutation this kills: dropping the awaitsReply consultation from
// grantFrameLineExtension. Every inbox-level test survives that mutation,
// because refuseUnsolicitedReplyLine catches the frame one step later — after
// the megabytes have already been read, copied and charged.
func TestSolicitedWideLineIsReadWholeAndUnsolicitedIsNot(t *testing.T) {
	t.Parallel()

	wire := oversizeLineOfType(t, "contacts", 1<<20)

	svc := &Service{}
	solicited := &peerSession{address: "10.9.9.9:64646"}
	defer solicited.admission.expectReply("contacts")()
	line, err := svc.readAdmittedSessionLine(bufio.NewReader(strings.NewReader(wire)), solicited)
	if err != nil {
		t.Fatalf("the reply to our own request was refused: %v", err)
	}
	if line != wire {
		t.Fatalf("the solicited reply came back as %d bytes, want %d", len(line), len(wire))
	}

	unsolicited := &peerSession{address: "10.9.9.9:64646"}
	line, err = svc.readAdmittedSessionLine(bufio.NewReader(strings.NewReader(wire)), unsolicited)
	if !errors.Is(err, errPeerSessionLineRefused) {
		t.Fatalf("the same line with no request outstanding: err = %v, want the refusal", err)
	}
	if line != "" {
		t.Fatalf("a refused line was materialised anyway: %d bytes", len(line))
	}
}

// TestStagedReadHonoursTheGrantedExtension is the negative control: the same
// reader must still deliver a line the gate paid for, whole. Without it the
// test above is satisfied by a reader that refuses everything.
func TestStagedReadHonoursTheGrantedExtension(t *testing.T) {
	t.Parallel()

	const softLimit = 8 << 10
	wire := `{"type":"contacts","pad":"` + strings.Repeat("p", 64<<10) + `"}` + "\n"
	reader := bufio.NewReaderSize(strings.NewReader(wire), 4<<10)

	read, err := readFrameLineStaged(reader, softLimit, func(string) int {
		return len(wire)
	})
	if err != nil {
		t.Fatalf("readFrameLineStaged: %v", err)
	}
	if !read.delimited {
		t.Fatal("a complete line must report its delimiter consumed")
	}
	if read.line != wire {
		t.Fatalf("the granted line came back truncated: %d bytes, want %d", len(read.line), len(wire))
	}
	if read.consumed != len(wire) {
		t.Fatalf("the read reports %d bytes consumed for a %d-byte line it delivered whole", read.consumed, len(wire))
	}
}

// TestRefusedLineDoesNotSwallowTheNextFrame pins the delimiter bookkeeping.
//
// A refused line leaves its remainder in the stream and the reader discards it
// — but the chunk that tripped the limit may already have carried the newline,
// and a discard that ran anyway would eat the NEXT frame. That failure is
// silent: the peer's traffic simply develops holes.
func TestRefusedLineDoesNotSwallowTheNextFrame(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	// Sized so the line ends exactly on a buffer boundary crossing: the pad is
	// MaxFrameLine so the delimiter lands in the same chunk that trips it.
	writeWireLine(t, peerEnd, oversizeLineOfType(t, "announce_routes", protocol.MaxFrameLine))
	requireNoInboxFrame(t, session, "an oversize announce frame")

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusal = %q, want ping: the discard ate it", frame.Type)
	}
}

// lineOfExactSize builds a well-formed single-type line of EXACTLY size bytes,
// the newline included.
//
// The exact length is what oversizeLineOfType cannot give: where the delimiter
// falls RELATIVE TO the reader's fill boundary decides whether the chunk that
// trips the limit already carries it, and that is the difference between a
// refusal with a remainder to discard and one with nothing left to discard.
func lineOfExactSize(t *testing.T, frameType string, size int) string {
	t.Helper()
	head := `{"type":"` + frameType + `","pad":"`
	const tail = `"}` + "\n"
	if size <= len(head)+len(tail) {
		t.Fatalf("a %d-byte line cannot carry the %d-byte envelope", size, len(head)+len(tail))
	}
	line := head + strings.Repeat("p", size-len(head)-len(tail)) + tail
	if len(line) != size {
		t.Fatalf("fixture is %d bytes, want exactly %d", len(line), size)
	}
	return line
}

// TestRefusedLineIsChargedTheBytesItActuallyRead is the accounting half of the
// refusal: with an oversize line now costing the neighbour bytes and nothing
// else on the datagram plane, the byte budget is the ONLY defence left against
// a neighbour that streams them — so it has to count honestly.
//
// The charge used to be RECONSTRUCTED from a constant — protocol.MaxFrameLine
// plus whatever the discard reported — and a reconstruction cannot know how far
// past the limit the read actually went. The reader stops on a BUFFER FILL, not
// on the limit, so:
//
//   - when the delimiter lands inside the chunk that crossed the limit there is
//     no remainder at all, the discard reports zero, and everything the crossing
//     chunk carried past the limit was read for free;
//   - when it lands further away the crossing chunk is charged to nobody for the
//     same reason.
//
// Both cases are here, and both assert the same invariant: the neighbour pays
// for exactly the bytes this node was made to read.
func TestRefusedLineIsChargedTheBytesItActuallyRead(t *testing.T) {
	t.Parallel()

	// A divisor of MaxFrameLine, so the strict limit falls exactly on a fill
	// boundary and the delimiter's position inside the crossing chunk is the
	// test's to choose rather than an accident of the envelope's length.
	const bufferSize = 4 << 10

	cases := map[string]int{
		"delimiter inside the crossing chunk": protocol.MaxFrameLine + bufferSize/2,
		"delimiter far past the limit":        protocol.MaxFrameLine + 3*bufferSize,
	}
	for name, size := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// A type with no entitlement to the wide budget, so the extension
			// gate answers zero and the refusal happens at the strict limit.
			wire := lineOfExactSize(t, "announce_routes", size)
			const next = `{"type":"ping"}` + "\n"

			svc := &Service{}
			session := &peerSession{address: "10.9.9.9:64646"}
			freezeSessionAdmission(t, session)
			reader := bufio.NewReaderSize(strings.NewReader(wire+next), bufferSize)

			if _, err := svc.readAdmittedSessionLine(reader, session); !errors.Is(err, errPeerSessionLineRefused) {
				t.Fatalf("readAdmittedSessionLine err = %v, want the refusal sentinel", err)
			}
			charged := peerSessionByteBurst - sessionRawBytesRemaining(session)
			if charged != float64(len(wire)) {
				t.Fatalf("the neighbour was charged %.0f bytes for a line of %d: the budget must bill what the read consumed, not what a constant says it did",
					charged, len(wire))
			}
			// The count is right for the right reason only if the stream is
			// where it says it is: a byte counted but not consumed is a byte of
			// the next frame left to be read as a frame of its own.
			line, err := readFrameLine(reader, protocol.MaxFrameLine)
			if err != nil || line != next {
				t.Fatalf("frame after the refusal = (%.32q, %v), want the ping that followed it", line, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// A violation costs the sender something, and a series costs it the session
// ---------------------------------------------------------------------------

// TestRepeatedOversizeFramesTearTheSessionDown is finding 2's second hole: a
// size violation used to be a silent per-frame drop with no cost at all, so a
// peer could repeat it at line rate forever.
//
// The contract has two halves and both are asserted: ONE violation is a drop
// and the session survives (the other tests in this package rely on that, and
// hasWideFrameLineBudget's named risk — a forgotten legitimate type — requires
// it), and a SERIES ends the session with an error that markPeerDisconnected
// records as `rate-limited` and sessionCloseCauseFromError attributes to the
// peer.
//
// The mutation this kills: drop without recording the violation.
func TestRepeatedOversizeFramesTearTheSessionDown(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	oversize := oversizeLineOfType(t, "announce_routes", protocol.MaxFrameLine)

	writeWireLine(t, peerEnd, oversize)
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("one violation must cost a frame and not the session; got %q", frame.Type)
	}

	for i := 0; i < peerSessionViolationBudget; i++ {
		writeWireLine(t, peerEnd, oversize)
	}

	select {
	case err := <-session.errCh:
		if code := protocol.ErrorCode(err); code != protocol.ErrCodeRateLimited {
			t.Fatalf("teardown error code = %q, want %q so markPeerDisconnected records it",
				code, protocol.ErrCodeRateLimited)
		}
		if sessionCloseCauseFromError(err) != sessionClosePeerInitiated {
			t.Fatal("an admission teardown must be attributed to the peer, or the disconnect_storm quarantine never sees it")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%d oversize frames did not end the session: a violation is still free", peerSessionViolationBudget+1)
	}
}

// ---------------------------------------------------------------------------
// The datagram plane pays its own budget and is not scored by this one
// ---------------------------------------------------------------------------

// TestDatagramStreamLeavesTheSharedBudgetToTheOtherPlanes is the finding of
// this round, and it is a statement about a frame the datagram stream never
// touched.
//
// Making the datagram's own refusal unscored was not enough, because the bucket
// it was charged to is SHARED. A stream of perfectly valid datagrams emptied it,
// and the next line of any other type — a `ping`, a file chunk, an announce —
// met an empty bucket, took the violation, and a series of them ended the
// session with `rate-limited`. The punishment was not removed, it was moved onto
// the neighbour's other traffic, which is precisely what "the two budgets are
// independent" (§5) forbids.
//
// The flood is sized to the bucket, not to the ledger: it costs strictly more
// than the bucket holds, so an implementation that charges a datagram here meets
// the `ping` with nothing left and drops it. The `ping` is therefore the
// assertion, and the datagram stream is only the setup.
//
// The POSITIVE CONTROL is the same session, the same frozen clock and the same
// bucket level, with the same NUMBER of lines of the same SIZE — differing only
// in the type. It still empties the bucket, the frame after it is still dropped
// and scored, and a series still ends the session. Without it the test would
// also pass for an implementation that simply switched the response-plane budget
// off, which is the defence this whole file exists to be.
func TestDatagramStreamLeavesTheSharedBudgetToTheOtherPlanes(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)
	freezeSessionAdmission(t, session)

	line := mustDatagramLine(t, newNodeDatagram(t, nil))
	if !isDatagramWireLine(line) {
		t.Fatal("the fixture is not classified as a datagram: the rule under test would never apply")
	}

	// One byte less than the stream would cost if the stream were charged here.
	const stream = 8
	budget := float64(stream*len(line)) - 1
	setSessionRawBudget(session, budget)

	for i := 0; i < stream; i++ {
		writeWireLine(t, peerEnd, line)
	}
	// The §5 budget is what the stream really pays, and waiting on it is also
	// what makes the assertion below race-free: the shared charge, if it
	// happened at all, would have happened before this counter moved.
	waitForAdmittedFrames(t, svc, stream)

	if spent := budget - sessionRawBytesRemaining(session); spent != 0 {
		t.Fatalf("a datagram stream spent %v bytes of the shared bucket: the response-plane budget belongs to the planes that have no budget of their own", spent)
	}

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("the frame after the datagram stream = %q, want ping: it paid for traffic of another plane", frame.Type)
	}
	if got := sessionViolations(session); got != 0 {
		t.Fatalf("the session carries %v violations after a stream of valid datagrams and one ping", got)
	}
	select {
	case err := <-session.errCh:
		t.Fatalf("a valid datagram stream ended the session: %v", err)
	default:
	}

	// The positive control: same size, same count, same bucket, not a datagram.
	control := sameSizeContactsLine(t, len(line))
	setSessionRawBudget(session, budget)
	for i := 0; i < stream; i++ {
		writeWireLine(t, peerEnd, control)
	}

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	waitForCondition(t, 5*time.Second, func() bool {
		return sessionViolations(session) >= 1
	})
	requireNoInboxFrame(t, session, "a ping behind a stream of non-datagram lines that emptied the shared bucket")

	// And the series still ends the session as `rate-limited` attributed to the
	// peer, which is what the score is FOR.
	for i := 0; i < peerSessionViolationBudget; i++ {
		writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	}
	select {
	case err := <-session.errCh:
		if code := protocol.ErrorCode(err); code != protocol.ErrCodeRateLimited {
			t.Fatalf("teardown error code = %q, want %q so markPeerDisconnected records it", code, protocol.ErrCodeRateLimited)
		}
		if sessionCloseCauseFromError(err) != sessionClosePeerInitiated {
			t.Fatal("an admission teardown must be attributed to the peer, or the disconnect_storm quarantine never sees it")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%d over-budget lines of a scored type did not end the session: the datagram rule leaked into the general case",
			peerSessionViolationBudget+1)
	}
}

// TestDatagramNeverSpendsTheResponsePlaneBudget is the same rule stated on the
// datagram itself, in both directions of the bucket.
//
// It REPLACES an assertion that pinned the defect. The previous revision
// required a datagram inside the budget to spend the shared byte bucket
// (`spent == len(line)`) and only exempted it from the LEDGER once the bucket
// was empty. That is the behaviour the finding is about: the metering was
// shared, so the cost was merely deferred onto whatever frame arrived next.
//
// What must hold instead:
//
//   - a datagram spends NOTHING of the shared bucket, whatever its level, and
//     is charged the §5 budget of its own plane on the raw wire line. "Not
//     charged here" is not "free": the assertion on AdmittedBytes is what
//     separates the two, and it is the one that fails for an implementation
//     that exempts a line from this budget without handing it to the other;
//   - with the shared bucket EMPTY the datagram still reaches the plane and
//     still scores nothing, while an ordinary line at the same moment is
//     dropped and scored. One violation on the ledger and not two is the whole
//     statement: the ping's, not the datagram's.
func TestDatagramNeverSpendsTheResponsePlaneBudget(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)
	freezeSessionAdmission(t, session)

	line := mustDatagramLine(t, newNodeDatagram(t, nil))

	rawBefore := sessionRawBytesRemaining(session)
	writeWireLine(t, peerEnd, line)
	waitForAdmittedFrames(t, svc, 1)
	if spent := rawBefore - sessionRawBytesRemaining(session); spent != 0 {
		t.Fatalf("a datagram spent %v bytes of the shared bucket: the two per-neighbour budgets replace each other, they do not stack", spent)
	}
	if charged := svc.datagramLayer().admission.Stats().AdmittedBytes; charged != uint64(len(line)) {
		t.Fatalf("the §5 budget was charged %d bytes, want the %d of the wire line: a line exempted here and metered nowhere is a free channel",
			charged, len(line))
	}

	// The bucket empty: the datagram is unaffected, the ordinary line is not.
	drainSessionRawBudget(session)
	writeWireLine(t, peerEnd, line)
	waitForAdmittedFrames(t, svc, 2)
	if got := datagramDropCount(svc, datagram.DropAdmission); got != 0 {
		t.Fatalf("the plane refused %d datagrams on its OWN budget: the fixture stopped exercising the response-plane rule it exists for", got)
	}
	if got := sessionViolations(session); got != 0 {
		t.Fatalf("a datagram scored %v violations on an empty shared bucket: exhausting a per-neighbour budget must cost the frame and nothing else", got)
	}

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	waitForCondition(t, 5*time.Second, func() bool {
		return sessionViolations(session) >= 1
	})
	if got := sessionViolations(session); got != 1 {
		t.Fatalf("the ledger holds %v violations, want exactly the 1 the ping earned", got)
	}
	select {
	case err := <-session.errCh:
		t.Fatalf("one violation ended the session: %v", err)
	default:
	}
}

// TestOversizeDatagramClaimIsASilentDropThatStillPays states what a line past
// protocol.MaxFrameLine claiming to be a datagram costs, on the ONE branch where
// the type is a claim rather than a classification.
//
// The rule this file used to hold — the claim is scored, and a series ends the
// session with `rate-limited` — punished the neighbour for a frame it did not
// write. A relay reads a line, refuses it, and forwards nothing; the SIZE of a
// frame it hands on is not a statement it made, so §2 keeps the refusal silent
// and §4.4 keeps punishment for the stable header and auth. The line is refused
// either way: the limit is not what changed.
//
// What replaces the ledger is the METER, and it is asserted here because a
// silent drop with no charge would be a free load channel: the bytes the
// neighbour made this node read are billed to the plane's own §5 per-neighbour
// budget BEFORE the refusal, so a stream of oversize claims drains exactly the
// bucket a stream of legal datagrams would — and drains the sender's own plane
// with it.
//
// The mutations this kills: scoring the refusal on the session ledger again
// (any of it — one violation or the teardown), and dropping the line without
// charging its bytes.
func TestOversizeDatagramClaimIsASilentDropThatStillPays(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)
	oversize := oversizeUnparseableDatagramLine(t)

	// One line first, so the ledger is asserted before a teardown can turn the
	// next write into a timeout instead of a verdict.
	before := svc.datagramLayer().admission.Stats().AdmittedBytes
	writeWireLine(t, peerEnd, oversize)
	waitForCondition(t, 5*time.Second, func() bool {
		return datagramDropCount(svc, datagram.DropFrameTooLarge) >= 1
	})
	if got := sessionViolations(session); got != 0 {
		t.Fatalf("one oversize datagram scored %v violations: a size verdict is not misbehaviour by the neighbour that relayed the frame", got)
	}

	// Then well past the ledger's tolerance: under the old rule the session was
	// gone before the last of these was written.
	const lines = peerSessionViolationBudget + 2
	for i := 1; i < lines; i++ {
		writeWireLine(t, peerEnd, oversize)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		return datagramDropCount(svc, datagram.DropFrameTooLarge) >= lines
	})

	if got := sessionViolations(session); got != 0 {
		t.Fatalf("the ledger holds %v violations for oversize datagrams: a size verdict is not misbehaviour by the neighbour that relayed the frame", got)
	}
	select {
	case err := <-session.errCh:
		t.Fatalf("a stream of oversize datagrams ended the session: %v", err)
	default:
	}

	// The bytes are not free: every refused line was billed to the plane's own
	// budget, on the raw bytes this node was made to read.
	charged := svc.datagramLayer().admission.Stats().AdmittedBytes - before
	if charged < uint64(lines*protocol.MaxFrameLine) {
		t.Fatalf("the §5 budget was charged %d bytes for %d refused lines, want at least %d: an uncharged refusal is a free load channel",
			charged, lines, lines*protocol.MaxFrameLine)
	}

	// And the session is still usable, which is the whole of "silent".
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusals = %q, want ping", frame.Type)
	}
}

// TestOversizeLineOfAnotherPlaneIsStillScored is the control that keeps the
// rule above from being read as "oversize is free".
//
// The carve-out belongs to the datagram plane and to the claim that names it:
// every other type on this reader keeps both its charge and its score, because
// the response plane has no per-neighbour byte budget of its own to move the
// cost to, and a violation there is still the only thing that stops a
// repetition.
func TestOversizeLineOfAnotherPlaneIsStillScored(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)
	oversize := oversizeLineOfType(t, "some_future_reply_v9", protocol.MaxFrameLine)

	writeWireLine(t, peerEnd, oversize)
	waitForCondition(t, 5*time.Second, func() bool {
		return sessionViolations(session) >= 1
	})

	for i := 0; i < peerSessionViolationBudget; i++ {
		writeWireLine(t, peerEnd, oversize)
	}
	select {
	case err := <-session.errCh:
		if code := protocol.ErrorCode(err); code != protocol.ErrCodeRateLimited {
			t.Fatalf("teardown error code = %q, want %q", code, protocol.ErrCodeRateLimited)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a repeated oversize line of an unenumerated type never ended the session")
	}
}

// TestDatagramBudgetReplacementAnswersFromTheAuthoritativeClassification pins
// the SOURCE of the replacement, which is the whole of its safety.
//
// The predicate now decides whether a line skips the shared bucket ENTIRELY, so
// every one of its three inputs is a way to buy a free channel if it is wrong:
//
//   - the classification has to be the same one the read loop's diversion
//     answers from, or a line could skip this budget without being handed to the
//     other one. It is literally the same call for that reason;
//   - the layer has to exist, or there is no replacement budget to skip to —
//     the same "no layer, no exemption" condition the inbound command-limiter
//     exemption carries;
//   - the neighbour has to be BILLABLE on the plane's own key. The §5 charge is
//     keyed on the address this node dialled, and handleDatagramFrame refuses a
//     zero key ABOVE its own charge, so a session with no address would divert
//     into a budget nobody pays.
func TestDatagramBudgetReplacementAnswersFromTheAuthoritativeClassification(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)
	session := &peerSession{address: "10.9.9.9:64646"}

	realLine := mustDatagramLine(t, newNodeDatagram(t, nil))
	lines := []string{
		realLine,
		`{"type":"messages"}` + "\n",
		// peekFrameType says `datagram`, encoding/json says something else or
		// nothing: neither may skip the shared budget.
		`{"type":"datagram","type":"messages"}` + "\n",
		`{"item":{"type":"datagram"}}` + "\n",
		`{"type":"datagram"}` + "\n",
		`{"type":"ping"}` + "\n",
	}
	for _, line := range lines {
		if got, want := svc.sessionDatagramPaysItsOwnBudget(session, line), isDatagramWireLine(line); got != want {
			t.Fatalf("sessionDatagramPaysItsOwnBudget(%.48q) = %v while the diversion says %v", line, got, want)
		}
	}

	// A session with no dialled address has no §5 budget key, so the shared
	// budget is the only one there is and the line pays it.
	unkeyed := &peerSession{}
	if svc.sessionDatagramPaysItsOwnBudget(unkeyed, realLine) {
		t.Fatal("a datagram skipped the response-plane budget on a session whose §5 key is the zero key: the line would be metered by nobody")
	}

	// With no layer there is no replacement budget either, and the response-plane
	// budget keeps both its meter and its ledger for a real datagram too.
	flagOff := newDatagramLayerService(t, false)
	if flagOff.sessionDatagramPaysItsOwnBudget(session, realLine) {
		t.Fatal("a datagram skipped the response-plane budget on a node with no layer to charge it")
	}
}

// ---------------------------------------------------------------------------
// `contacts` is a reply, and a reply needs a request
// ---------------------------------------------------------------------------

// TestUnsolicitedContactsIsRefusedAtAnySize is finding 2's third hole.
//
// `contacts` is the reply to `fetch_contacts` and the batching reply the wide
// 8 MiB budget exists for. It was accepted — and fully decoded — with no
// request outstanding, at any size, from any authenticated peer, and the
// dispatcher then had no case for it and dropped it: the node paying for a
// parse whose only possible outcome was a no-op.
//
// Both sizes are asserted because they fail differently. The SMALL one proves
// the refusal is not a size gate in disguise: it is a well-formed frame that
// reached the session inbox before this rule. The WIDE one is the one that
// mattered: it bought eight megabytes of read, copy and decode per frame.
//
// The mutation this kills: admitting `contacts` without consulting
// awaitsReply.
func TestUnsolicitedContactsIsRefusedAtAnySize(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	small, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:     "contacts",
		Contacts: []protocol.ContactFrame{{Address: "a", PubKey: "b", BoxKey: "c", BoxSig: "d"}},
	})
	if err != nil {
		t.Fatalf("MarshalFrameLine: %v", err)
	}
	if wireLineBudget(small) > protocol.MaxFrameLine {
		t.Fatal("the small fixture must be inside the strict budget, or it proves nothing about size independence")
	}

	writeWireLine(t, peerEnd, small)
	requireNoInboxFrame(t, session, "a small `contacts` reply nobody requested")

	writeWireLine(t, peerEnd, oversizeLineOfType(t, "contacts", protocol.MaxFrameLine))
	requireNoInboxFrame(t, session, "an oversize `contacts` reply nobody requested")

	// The session is still alive for ordinary traffic.
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusals = %q, want ping", frame.Type)
	}
}

// TestSolicitedContactsPassesAtTheWideBudget is the negative control that stops
// the rule above from being a regression: while a `fetch_contacts` is
// outstanding, a large `contacts` reply is exactly what the wide budget was
// raised for and must still arrive whole.
func TestSolicitedContactsPassesAtTheWideBudget(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)
	defer session.admission.expectReply("contacts")()

	line, err := protocol.MarshalFrameLine(oversizeContactsFrame())
	if err != nil {
		t.Fatalf("MarshalFrameLine: %v", err)
	}
	if wireLineBudget(line) <= protocol.MaxFrameLine {
		t.Fatal("the fixture does not exercise the wide budget")
	}

	writeWireLine(t, peerEnd, line)
	if frame := awaitInboxFrame(t, session); frame.Type != "contacts" {
		t.Fatalf("the reply to our own fetch_contacts came back as %q", frame.Type)
	}
}

// TestPeerSessionRequestRegistersTheOutstandingReply pins WHERE the outstanding
// request is recorded, because the whole rule above is unexpressible without it
// and the two sides live on different goroutines: peerSessionRequest waits on
// the serve loop while readPeerSession decides on the reader.
func TestPeerSessionRequestRegistersTheOutstandingReply(t *testing.T) {
	t.Parallel()

	session := &peerSession{}
	if session.admission.awaitsReply("contacts") {
		t.Fatal("a fresh session must await nothing")
	}
	release := session.admission.expectReply("contacts")
	if !session.admission.awaitsReply("contacts") {
		t.Fatal("expectReply did not register the outstanding reply")
	}
	if session.admission.awaitsReply("peers") {
		t.Fatal("awaitsReply must answer for the registered type only")
	}
	release()
	if session.admission.awaitsReply("contacts") {
		t.Fatal("the release did not clear the outstanding reply")
	}
}

// ---------------------------------------------------------------------------
// `push_message` is unsolicited by design and needs a budget of its own
// ---------------------------------------------------------------------------

// TestUnsolicitedPushHasItsOwnBudget pins the sub-budget.
//
// `push_message` is the only member of hasWideFrameLineBudget that arrives with
// no request behind it, so "do not ask for it" is not a defence and the shared
// byte budget alone would let a peer spend the WHOLE neighbour allowance on
// pushes — 256 inbox slots of up to 8 MiB each.
//
// The assertion is discriminating on purpose: with the push bucket empty and
// the shared bucket untouched, a push is dropped while a frame of the same size
// that is not a push still arrives. A mutation that deletes the push bucket, or
// charges it from the shared one, fails on the first half; a mutation that
// drains the shared budget instead fails on the second.
func TestUnsolicitedPushHasItsOwnBudget(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)
	freezeSessionAdmission(t, session)
	drainSessionPushBudget(session)

	push, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:      "push_message",
		Topic:     "dm",
		Recipient: "r",
		Item:      &protocol.MessageFrame{ID: "m1", Sender: "s", Body: strings.Repeat("B", 1024)},
	})
	if err != nil {
		t.Fatalf("MarshalFrameLine: %v", err)
	}

	writeWireLine(t, peerEnd, push)
	requireNoInboxFrame(t, session, "a push_message with the push budget spent")

	if remaining := sessionRawBytesRemaining(session); remaining <= 0 {
		t.Fatalf("the shared byte budget was drained too (%v): the test no longer discriminates", remaining)
	}
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("the push sub-budget throttled unrelated traffic: got %q", frame.Type)
	}
}

// TestPushWithinItsBudgetStillDelivers is the negative control for the one path
// that carries every DM on this reader. A regression here loses messages.
func TestPushWithinItsBudgetStillDelivers(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	push, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:      "push_message",
		Topic:     "dm",
		Recipient: "r",
		Item:      &protocol.MessageFrame{ID: "m1", Sender: "s", Body: strings.Repeat("B", maxPeerCommandBodyBytes)},
	})
	if err != nil {
		t.Fatalf("MarshalFrameLine: %v", err)
	}
	if wireLineBudget(push) <= protocol.MaxFrameLine {
		t.Fatal("the largest ACCEPTED push must be an oversize line, or this test misses the wide path")
	}

	writeWireLine(t, peerEnd, push)
	if frame := awaitInboxFrame(t, session); frame.Type != "push_message" {
		t.Fatalf("a maximum-size DM was dropped: got %q", frame.Type)
	}
}

// ---------------------------------------------------------------------------
// Ordinary traffic is not degraded
// ---------------------------------------------------------------------------

// TestOrdinaryTrafficIsNotCharged pins the regression risk this whole file
// carries: the peer-session reader is the hot path of the entire network, and a
// budget that fires on honest traffic is worse than the finding it closes.
//
// A burst well past what any single exchange produces has to pass with no
// violation recorded at all.
func TestOrdinaryTrafficIsNotCharged(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	const burst = 64
	for i := 0; i < burst; i++ {
		writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
		if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
			t.Fatalf("frame %d of an ordinary burst = %q", i, frame.Type)
		}
	}
	if got := sessionViolations(session); got != 0 {
		t.Fatalf("an ordinary burst recorded %v violations", got)
	}
	select {
	case err := <-session.errCh:
		t.Fatalf("an ordinary burst tore the session down: %v", err)
	default:
	}
}

// ---------------------------------------------------------------------------
// The controller itself
// ---------------------------------------------------------------------------

// TestPeerSessionAdmissionBucketsRefillAndForgive states the budget arithmetic
// on the controller, so every dimension is pinned in one place rather than only
// where a fixture happens to reach it.
func TestPeerSessionAdmissionBucketsRefillAndForgive(t *testing.T) {
	t.Parallel()

	now := time.Now()
	var admission peerSessionAdmission
	admission.setClock(func() time.Time { return now })

	// The zero value is a live controller: the first charge finds full buckets.
	if !admission.chargeLine(peerSessionByteBurst, false) {
		t.Fatal("the zero-value controller refused its first line: a session would start with no budget")
	}
	if admission.chargeLine(1, false) {
		t.Fatal("a spent byte bucket still admitted a line")
	}

	// Refill is continuous, and one second buys one second of rate.
	now = now.Add(time.Second)
	if !admission.chargeLine(1, false) {
		t.Fatal("the byte bucket did not refill")
	}

	// The violation ledger decays rather than accumulating for the life of the
	// session, or a peer that misbehaves once an hour eventually loses it.
	for i := 0; i < peerSessionViolationBudget; i++ {
		if _, tearDown := admission.recordViolation(); tearDown {
			t.Fatalf("violation %d tore the session down inside the tolerated budget", i+1)
		}
	}
	now = now.Add(time.Duration(float64(time.Second) * ((peerSessionViolationBudget + 1) / peerSessionViolationDecayPerSecond)))
	if score, tearDown := admission.recordViolation(); tearDown || score > 1 {
		t.Fatalf("the ledger did not decay: score %v, tearDown %v", score, tearDown)
	}
}

// TestClaimedFrameTypeFromPrefixReadsOnlyWhatItCanTrust pins the pre-read
// classification, which is the one input the extension gate has.
//
// It may only ever BUY the wide budget, so the shapes it must refuse are the
// ones where a claim would be a lie the reader cannot check: a nested key, an
// escaped key or value, a non-string value, a prefix that names nothing yet.
func TestClaimedFrameTypeFromPrefixReadsOnlyWhatItCanTrust(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		prefix string
		want   string
		named  bool
	}{
		"plain claim":       {prefix: `{"type":"contacts","pad":"aaa`, want: "contacts", named: true},
		"spaced claim":      {prefix: `{ "type" : "push_message" , "x`, want: "push_message", named: true},
		"nested first":      {prefix: `{"a":{"type":"contacts"},"pad":"aaa`, named: false},
		"escaped key":       {prefix: `{"ty\u0070e":"contacts","pad":"a`, named: false},
		"escaped value":     {prefix: `{"type":"cont\u0061cts","pad":"a`, named: false},
		"non-string value":  {prefix: `{"type":null,"pad":"aaaaaaaaaaaa`, named: false},
		"nothing yet":       {prefix: `{"pad":"aaaaaaaaaaaaaaaaaaaaaaaa`, named: false},
		"not an object":     {prefix: `["type","contacts"`, named: false},
		"truncated in name": {prefix: `{"type":"cont`, named: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, named := claimedFrameTypeFromPrefix(tc.prefix)
			if named != tc.named || (named && got != tc.want) {
				t.Fatalf("claimedFrameTypeFromPrefix(%q) = (%q, %v), want (%q, %v)", tc.prefix, got, named, tc.want, tc.named)
			}
		})
	}
}
