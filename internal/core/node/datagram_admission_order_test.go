package node

import (
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_admission_order_test.go pins §4.1 step 1 — "admission by bytes and
// frames, BEFORE any decoding" — on the two paths that broke it:
//
//   - the pre-parse classifier could be STEERED, so an oversize line bought
//     the 8 MiB response budget from the scan and delivered a strict-budget
//     type to the parser;
//   - an ordinary datagram inside 128 KiB met protocol.ParseFrameLine before
//     the layer's budget, so a neighbour got a full JSON unmarshal for free on
//     both receive paths.

// ---------------------------------------------------------------------------
// The classifier cannot be steered (§4.1 step 1)
// ---------------------------------------------------------------------------

// TestTopLevelFrameTypeRefusesAnAmbiguousLine states the rule the budget gate
// and the datagram diversion both rest on: a pre-parse classification is
// trustworthy only when the line names its type ONCE, at the top level, in
// plain characters.
//
// Every "want false" row below is a line on which peekFrameType and
// encoding/json return DIFFERENT answers, which is the whole reason this
// function exists.
func TestTopLevelFrameTypeRefusesAnAmbiguousLine(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		line  string
		want  string
		named bool
	}{
		"plain": {
			line: `{"type":"messages","count":1}`, want: "messages", named: true,
		},
		"not the first key": {
			line: `{"count":1,"type":"messages"}`, want: "messages", named: true,
		},
		"whitespace around the colon": {
			line: "{\"type\" : \"messages\"}", want: "messages", named: true,
		},
		"duplicate top-level type": {
			// peekFrameType says `messages`, encoding/json says `datagram`.
			line: `{"type":"messages","pad":"x","type":"datagram"}`, named: false,
		},
		"nested type seen first": {
			// peekFrameType says `messages` — the NESTED key — while
			// encoding/json says `datagram`. The scan follows the parser: a key
			// below the top level names nothing.
			line: `{"item":{"type":"messages"},"type":"datagram"}`, want: "datagram", named: true,
		},
		"type inside an array element": {
			line: `{"items":[{"type":"messages"}],"type":"datagram"}`, want: "datagram", named: true,
		},
		"only a nested type": {
			// Nothing at the top level names the frame, so neither does this.
			line: `{"item":{"type":"messages"}}`, named: false,
		},
		"escaped key": {
			// encoding/json decodes \u0070 to `p` and reads the key as `type`;
			// a raw-byte comparison would miss it, so an escaped top-level key
			// is refused outright.
			line: `{"ty\u0070e":"datagram","count":1}`, named: false,
		},
		"escaped value": {
			line: `{"type":"\u0064atagram"}`, named: false,
		},
		"no type at all": {
			line: `{"count":1}`, named: false,
		},
		"type is not a string": {
			line: `{"type":7}`, named: false,
		},
		"not an object": {
			line: `["type","datagram"]`, named: false,
		},
		"unterminated object": {
			line: `{"type":"datagram"`, named: false,
		},
		"a value that merely contains the key text": {
			// The `"type"` bytes appear inside a string VALUE, escaped, which
			// is the only way JSON can carry them there.
			line: `{"note":"\"type\":\"datagram\"","type":"messages"}`, want: "messages", named: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, named := topLevelFrameType(tc.line)
			if named != tc.named {
				t.Fatalf("topLevelFrameType(%s) named = %v, want %v (value %q)", tc.line, named, tc.named, got)
			}
			if named && got != tc.want {
				t.Fatalf("topLevelFrameType(%s) = %q, want %q", tc.line, got, tc.want)
			}
		})
	}
}

// TestDuplicateTypeKeyLosesTheWideBudget is finding 1, hole 1.
//
// `{"type":"messages", …, "type":"datagram"}` is one line with two readers and
// two answers: the cheap scan takes the first key, encoding/json takes the
// last. On the peer-session reader that is the difference between the 8 MiB
// response budget and the strict 128 KiB one, so the decoy bought a full
// unmarshal of up to eight megabytes — the exact work §4.1 step 1 forbids a
// neighbour to impose.
//
// The test asserts the divergence is REAL first, so it cannot pass because the
// fixture stopped being a decoy, and then asserts the gate refuses it.
//
// The mutation this kills: classifying with peekFrameType inside
// admitFrameLinePreParse.
func TestDuplicateTypeKeyLosesTheWideBudget(t *testing.T) {
	t.Parallel()

	padding := strings.Repeat("r", protocol.MaxFrameLine)
	decoys := map[string]string{
		"duplicate top-level key": `{"type":"contacts","pad":"` + padding + `","type":"datagram"}` + "\n",
		"nested key seen first":   `{"item":{"type":"contacts"},"pad":"` + padding + `","type":"datagram"}` + "\n",
	}
	for name, decoy := range decoys {
		t.Run(name, func(t *testing.T) {
			if peeked := peekFrameType(decoy); peeked != "contacts" {
				t.Fatalf("fixture is not a decoy: peekFrameType = %q, want the entitled type", peeked)
			}
			frame, err := protocol.ParseFrameLine(strings.TrimSpace(decoy))
			if err != nil || frame.Type != protocol.DatagramFrameType {
				t.Fatalf("fixture is not a decoy: the parser sees %q (err %v), want datagram", frame.Type, err)
			}
			if wireLineBudget(decoy) <= protocol.MaxFrameLine {
				t.Fatal("fixture must exceed MaxFrameLine")
			}
			if _, verdict := admitFrameLinePreParse(decoy); verdict == preParseAdmit {
				t.Fatal("an oversize line naming its type twice earned the 8 MiB budget: the node decodes it before anything looks at the real type")
			}
		})
	}

	// The negative control: an honest oversize response frame of an ENTITLED
	// type keeps its wide budget. A gate that refused everything large would
	// pass the assertions above and break batched replies.
	honest := `{"type":"contacts","pad":"` + padding + `"}` + "\n"
	if _, verdict := admitFrameLinePreParse(honest); verdict != preParseAdmit {
		t.Fatal("an unambiguous oversize `contacts` reply lost the response budget it is entitled to")
	}
}

// TestPeerSessionRefusesTheDuplicateTypeDecoyUnparsed is the same finding on
// the production reader: the decoy must be dropped by the loop, not handed to
// the dispatcher, and the session must survive.
func TestPeerSessionRefusesTheDuplicateTypeDecoyUnparsed(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)

	decoy := `{"type":"contacts","pad":"` + strings.Repeat("r", protocol.MaxFrameLine) +
		`","type":"datagram"}` + "\n"
	writeWireLine(t, peerEnd, decoy)
	requireNoInboxFrame(t, session, "an oversize line that showed the scan an entitled type and the parser a datagram")

	// The CONVEYOR never saw it — the refusal is now one step above even the
	// classification, inside the read — while the neighbour still paid for the
	// bytes it made this node read and for the refusal itself (§4.1 step 1; see
	// peer_session_admission.go for why an unmetered refusal is a free load
	// channel). The charge lands on the RESPONSE-plane budget rather than the
	// datagram one: this reader has a per-neighbour budget of its own now, so it
	// no longer has to bill a line that was never a datagram to the datagram
	// plane. The VIOLATION LEDGER is the observable and the byte bucket is not,
	// because the byte bucket refills in real time and the ledger does not.
	waitForCondition(t, 5*time.Second, func() bool {
		return sessionViolations(session) >= 1
	})
	if got := datagramObservedCount(svc); got != 0 {
		t.Fatalf("the conveyor decided on %d frames from a line refused before the parse", got)
	}

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusal = %q, want ping", frame.Type)
	}
}

// ---------------------------------------------------------------------------
// The budget is charged before the universal parser (§4.1 step 1)
// ---------------------------------------------------------------------------

// parserHostileDatagramLine is a line that is UNAMBIGUOUSLY a datagram, is
// inside the strict 128 KiB budget, and which protocol.ParseFrameLine refuses.
//
// It is what separates the two orders. A well-formed datagram is charged
// whichever way round the parse and the budget are, so it cannot tell them
// apart; this one can. With the universal parser first the line is dropped by
// it and the neighbour pays nothing, which is precisely the free decode §4.1
// step 1 exists to prevent. With the budget first the layer charges the bytes
// and then refuses the frame on its own strict parser.
func parserHostileDatagramLine(t *testing.T) string {
	t.Helper()
	// `capabilities` is a []string on the universal Frame, so a number there
	// is a decode error — while the datagram wire form has no such field at
	// all and the strict parser refuses it for its own reasons.
	line := `{"type":"datagram","capabilities":7}` + "\n"
	if _, err := protocol.ParseFrameLine(strings.TrimSpace(line)); err == nil {
		t.Fatal("fixture parses: it cannot distinguish a charge before the parse from one after it")
	}
	if !isDatagramWireLine(line) {
		t.Fatal("fixture is not classified as a datagram: the diversion under test would not apply")
	}
	if wireLineBudget(line) > protocol.MaxFrameLine {
		t.Fatal("fixture must be an ORDINARY datagram, inside the strict budget")
	}
	return line
}

// TestInboundDatagramIsChargedBeforeTheUniversalParser is finding 1, hole 2,
// on the accepted-connection path.
//
// The mutation this kills: removing the isDatagramWireLine diversion from
// dispatchNetworkFrame, so protocol.ParseFrameLine runs first.
func TestInboundDatagramIsChargedBeforeTheUniversalParser(t *testing.T) {
	svc, _, connID := newLayeredDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	line := parserHostileDatagramLine(t)

	if !svc.dispatchNetworkFrame(connID, strings.TrimSuffix(line, "\n")) {
		t.Fatal("a datagram tore the connection down; §2 makes every refusal a silent drop")
	}

	stats := svc.datagramLayer().admission.Stats()
	if stats.Admitted != 1 {
		t.Fatalf("the neighbour was charged for %d frames, want 1: the universal parser swallowed the line before the budget ran", stats.Admitted)
	}
	if stats.AdmittedBytes == 0 {
		t.Fatal("the neighbour was charged no bytes: §4.1 step 1 counts the whole wire line")
	}
}

// TestOutboundSessionDatagramIsChargedBeforeTheUniversalParser is the same
// hole on the peer-session reader, which is the direction that matters most:
// that reader accepts up to 8 MiB.
//
// The mutation this kills: removing the isDatagramWireLine diversion from
// readPeerSession, so the line goes through protocol.ParseFrameLine and the
// inbox first.
func TestOutboundSessionDatagramIsChargedBeforeTheUniversalParser(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)
	writeWireLine(t, peerEnd, parserHostileDatagramLine(t))

	requireNoInboxFrame(t, session, "a datagram must never travel through the universal Frame inbox")
	waitForAdmittedFrames(t, svc, 1)
}

// TestOrdinaryDatagramReachesTheConveyorThroughTheDiversion is the positive
// control for both paths above: the diversion must not have turned a working
// receive path into a drop. A well-formed datagram still reaches the strict
// parser and is still observed by the layer.
func TestOrdinaryDatagramReachesTheConveyorThroughTheDiversion(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)
	line := mustDatagramLine(t, newNodeDatagram(t, nil))
	if !isDatagramWireLine(line) {
		t.Fatal("a real datagram line is not classified as one: the diversion would never fire in production")
	}

	writeWireLine(t, peerEnd, line)
	waitForAdmittedFrames(t, svc, 1)
	waitForObservedFrames(t, svc, 1)
	requireNoInboxFrame(t, session, "a datagram reached the universal inbox instead of the ingress")
}

// waitForAdmittedFrames waits for the layer's admission counter to reach want.
// The receive loop under test runs in its own goroutine, so the counter is the
// synchronisation point rather than a bare read.
func waitForAdmittedFrames(t *testing.T, svc *Service, want uint64) {
	t.Helper()
	waitForDatagramCounter(t, want, "frames charged to the neighbour", func() uint64 {
		return svc.datagramLayer().admission.Stats().Admitted
	})
}

// waitForObservedFrames waits for the CONVEYOR to have seen want frames.
//
// It is a second wait and not a read after waitForAdmittedFrames, because the
// two counters move at different points of §4.1: admission is step 1 and the
// observation is the verdict at the end, so a test that read the second right
// after the first would race the conveyor by exactly one parse.
func waitForObservedFrames(t *testing.T, svc *Service, want uint64) {
	t.Helper()
	waitForDatagramCounter(t, want, "frames observed by the conveyor", func() uint64 {
		return datagramObservedCount(svc)
	})
}

func waitForDatagramCounter(t *testing.T, want uint64, what string, get func() uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got uint64
	for time.Now().Before(deadline) {
		if got = get(); got >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s = %d, want %d", what, got, want)
}

// ---------------------------------------------------------------------------
// One classification, two decisions
// ---------------------------------------------------------------------------

// TestCommandLimiterExemptionMatchesTheDiversion pins that the rate-limiter
// exemption and the pre-parse diversion answer from the SAME classification.
//
// They must, or a line that merely looks like a datagram leaves the command
// limiter — because it looks like one — and never reaches the layer's budget
// either, because it is not one. That gap is a frame charged by nobody.
func TestCommandLimiterExemptionMatchesTheDiversion(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)

	// None of these lines is file_command or a bulk announce frame, so on this
	// set the read loop's whole exemption decision IS its datagram half — which
	// is what makes the equality below an assertion about one classification
	// rather than a coincidence.
	lines := []string{
		mustDatagramLine(t, newNodeDatagram(t, nil)),
		`{"type":"messages"}` + "\n",
		`{"type":"messages","type":"datagram"}` + "\n",
		`{"item":{"type":"datagram"}}` + "\n",
		`{"type":"ping"}` + "\n",
	}
	// The connection is AUTHENTICATED: the exemption also asks whether the §5
	// budget has a key to charge, and this test is about the classification.
	authenticated := registerDatagramCommandConn(t, svc, domain.ConnID(8821), true)
	for _, line := range lines {
		if got, want := svc.frameLineExemptFromCommandLimit(authenticated, line), isDatagramWireLine(line); got != want {
			t.Fatalf("frameLineExemptFromCommandLimit(%q) = %v while the diversion says %v", line, got, want)
		}
	}

	// And with no layer there is no budget to charge, so the exemption is off
	// for a real datagram too.
	flagOff := newDatagramLayerService(t, false)
	flagOffConn := registerDatagramCommandConn(t, flagOff, domain.ConnID(8822), true)
	if flagOff.frameLineExemptFromCommandLimit(flagOffConn, mustDatagramLine(t, newNodeDatagram(t, nil))) {
		t.Fatal("a datagram was exempted from the command limiter on a node with no layer to charge it")
	}
}
