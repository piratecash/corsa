package node

import (
	"bufio"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// peer_session_admission_test.go covers the ORDER of the gates in the outbound
// peer-session read loop and the shape of the line the datagram plane is
// handed, on both receive directions.
//
// The two obligations pinned here are:
//
//   - §4.1 step 1 / §2.3 (spec lines 278-281, 1074): the strict MaxFrameLine
//     budget is an ADMISSION check. It has to refuse before anything is
//     parsed, because the peer-session reader itself accepts up to
//     maxResponseLineBytes (8 MiB) and a gate below protocol.ParseFrameLine
//     lets a peer make this node decode eight megabytes of JSON per frame
//     first;
//   - §3.4 / §5 (spec lines 372-381): the datagram plane counts and parses the
//     SAME bytes the wire delivered. A trimmed line under-charges the
//     neighbour's byte budget by every byte of outside whitespace and hands
//     the strict parser something the wire never carried.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// newReadPeerSessionFixture wires a Service with the conveyor and one
// authenticated outbound session, and returns the pipe end a "peer" writes
// wire lines into plus the session the real readPeerSession loop is driving.
//
// The loop is the production one: nothing here paraphrases the reader, which
// is the whole point — the gate order under test only exists inside it.
func newReadPeerSessionFixture(t *testing.T) (*Service, net.Conn, *peerSession) {
	t.Helper()

	svc := newDatagramLayerService(t, true)
	if svc.datagramLayer() == nil {
		t.Fatal("the fixture must build the conveyor: the ingress under test only exists with one")
	}
	svc.runCtx = context.Background()
	requireDatagramPlane(t, svc)
	registerFixtureDatagramTypes(t, svc)

	address := domain.PeerAddress("10.9.2.1:64646")
	session := &peerSession{
		address:      address,
		peerIdentity: domain.PeerIdentityFromWire(datagramTestDstHex),
		connID:       domain.ConnID(9200),
		capabilities: []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
		sendCh:       make(chan peerSendItem, 4),
		inboxCh:      make(chan protocol.Frame, 8),
		errCh:        make(chan error, 4),
		authOK:       true,
	}
	svc.peerMu.Lock()
	svc.sessions[address] = session
	svc.health[address] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	peerEnd, nodeEnd := net.Pipe()
	t.Cleanup(func() { _ = peerEnd.Close() })
	t.Cleanup(func() { _ = nodeEnd.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.readPeerSession(bufio.NewReader(nodeEnd), session)
	}()
	t.Cleanup(func() {
		_ = nodeEnd.Close()
		_ = peerEnd.Close()
		<-done
	})

	return svc, peerEnd, session
}

// writeWireLine feeds one line to the reader under test exactly as a peer
// would, and fails the test rather than hanging if the loop stopped reading.
func writeWireLine(t *testing.T, peerEnd net.Conn, line string) {
	t.Helper()
	if err := peerEnd.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if _, err := peerEnd.Write([]byte(line)); err != nil {
		t.Fatalf("write wire line: %v", err)
	}
}

// awaitInboxFrame waits for the read loop to hand one frame to the session
// inbox. A timeout means the frame was dropped somewhere in the loop.
func awaitInboxFrame(t *testing.T, session *peerSession) protocol.Frame {
	t.Helper()
	select {
	case frame := <-session.inboxCh:
		return frame
	case <-time.After(5 * time.Second):
		t.Fatal("no frame reached the session inbox")
		return protocol.Frame{}
	}
}

// requireNoInboxFrame asserts the loop dropped everything it has read so far.
func requireNoInboxFrame(t *testing.T, session *peerSession, why string) {
	t.Helper()
	select {
	case frame := <-session.inboxCh:
		t.Fatalf("%s: frame %q reached the dispatcher", why, frame.Type)
	case <-time.After(200 * time.Millisecond):
	}
}

// oversizeUnparseableDatagramLine is the discriminating fixture of this file:
// a line past MaxFrameLine that announces itself as a datagram and is NOT
// valid JSON.
//
// It is what separates "the budget refused it" from "the parser refused it".
// A well-formed oversize line is refused either way, so it cannot tell the two
// orders apart; this one can, because protocol.ParseFrameLine rejects it and
// would swallow the frame silently before any budget was consulted.
func oversizeUnparseableDatagramLine(t *testing.T) string {
	t.Helper()
	line := `{"type":"datagram","payload":"` + strings.Repeat("A", protocol.MaxFrameLine) + `"` + "\n"
	if wireLineBudget(line) <= protocol.MaxFrameLine {
		t.Fatalf("fixture is only %d bytes, it must exceed MaxFrameLine", wireLineBudget(line))
	}
	if _, err := protocol.ParseFrameLine(strings.TrimSpace(line)); err == nil {
		t.Fatal("fixture parses: it cannot distinguish a budget refusal from a parser refusal")
	}
	return line
}

// ---------------------------------------------------------------------------
// §4.1 step 1 — admission before any parsing
// ---------------------------------------------------------------------------

// TestPeerSessionRefusesOversizeDatagramBeforeParsing is the ordering test.
//
// The frame is unparseable on purpose. With the budget gate ABOVE
// protocol.ParseFrameLine the refusal is the budget's and shows up on the
// frame_too_large counter; with the gate back below the parser the line never
// reaches it — ParseFrameLine fails first and the loop continues — and the
// counter stays at zero. That is the mutation this test exists to kill.
func TestPeerSessionRefusesOversizeDatagramBeforeParsing(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)
	writeWireLine(t, peerEnd, oversizeUnparseableDatagramLine(t))

	waitForCondition(t, 5*time.Second, func() bool {
		return datagramDropCount(svc, datagram.DropFrameTooLarge) == 1
	})

	// The session survives: an oversize frame is a silent drop, never a
	// tear-down, and the next legal frame still gets through.
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusal = %q, want ping", frame.Type)
	}
	select {
	case err := <-session.errCh:
		t.Fatalf("the oversize frame tore the session down: %v", err)
	default:
	}
}

// unclassifiedOversizeLine returns a line past MaxFrameLine that the pre-parse
// classification refuses to read, but which parses cleanly as a type that IS
// entitled to the wide 8 MiB response budget.
//
// The obfuscation is a DUPLICATE top-level `type` key. It is the shape that
// makes a pre-parse classifier and encoding/json disagree by construction — the
// scan takes the first, the parser takes the last — so topLevelFrameType
// declines to answer at all, which is the whole reason an unidentified line may
// not be handed the wide budget.
func unclassifiedOversizeLine(t *testing.T, realType string) string {
	t.Helper()
	line := `{"type":"` + realType + `","pad":"` +
		strings.Repeat("r", protocol.MaxFrameLine) + `","type":"` + realType + `"}` + "\n"
	if _, named := topLevelFrameType(line); named {
		t.Fatal("fixture does not obfuscate: the pre-parse scan read its type")
	}
	if wireLineBudget(line) <= protocol.MaxFrameLine {
		t.Fatal("fixture must exceed MaxFrameLine")
	}
	if frame, err := protocol.ParseFrameLine(strings.TrimSpace(line)); err != nil || frame.Type != realType {
		t.Fatalf("fixture must parse as %q; got %q, err %v", realType, frame.Type, err)
	}
	return line
}

// TestPeerSessionRefusesUnclassifiedOversizeLineBeforeParsing is the §4.1
// step 1 obligation stated as the rule the budget really is: the wide
// maxResponseLineBytes budget is earned by a POSITIVELY IDENTIFIED type that is
// entitled to it, not granted by default to whatever the scan failed to read.
//
// The fixture parses as `ping` — a type the strict 128 KiB budget does not
// cover — so before this rule the line was admitted, unmarshalled in full and
// handed to the dispatcher: a neighbour hiding its `"type"` key behind a decoy
// bought itself up to 8 MiB of JSON decoding per frame, which is exactly the
// work §4.1 step 1 forbids it to be able to impose. The refusal now happens on
// the bytes in hand, before protocol.ParseFrameLine is reached.
//
// The mutation this kills: granting the wide budget to an unidentified line
// (`!named → admit`) puts the frame back in the inbox.
func TestPeerSessionRefusesUnclassifiedOversizeLineBeforeParsing(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	writeWireLine(t, peerEnd, unclassifiedOversizeLine(t, "ping"))
	requireNoInboxFrame(t, session, "an oversize line whose type the pre-parse scan could not identify")

	// The refusal is silent and the session survives — an oversize frame is a
	// drop, never a tear-down.
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusal = %q, want ping", frame.Type)
	}
	select {
	case err := <-session.errCh:
		t.Fatalf("the refused line tore the session down: %v", err)
	default:
	}
}

// TestPeerSessionOversizeGateSurvivesTypeObfuscation pins the outcome for the
// obfuscation the pre-parse gate was blind to.
//
// peekFrameType takes the first `"type":"…"` it finds, so a peer can put an
// ENTITLED type first and the real one second: the scan read `messages`, the
// line earned the wide budget and was parsed, and only the authoritative check
// on protocol.Frame.Type — `announce_routes`, a strict 128 KiB type — caught
// it, after eight megabytes of decoding nobody charged for.
//
// The pre-parse gate now declines to classify a line that names its type twice
// (topLevelFrameType), so this decoy is refused unparsed. The authoritative
// check below the parser stays where it is: it is the one that answers for a
// line whose type only the parser can resolve, and this test pins the OUTCOME —
// the frame never reaches the dispatcher — rather than which of the two gates
// produced it.
func TestPeerSessionOversizeGateSurvivesTypeObfuscation(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	decoy := `{"type":"messages","pad":"` +
		strings.Repeat("r", protocol.MaxFrameLine) + `","type":"announce_routes"}` + "\n"
	if peeked := peekFrameType(decoy); peeked != "messages" {
		t.Fatalf("fixture must be classified as the entitled type; peekFrameType = %q", peeked)
	}
	if wireLineBudget(decoy) <= protocol.MaxFrameLine {
		t.Fatal("fixture must exceed MaxFrameLine")
	}
	if frame, err := protocol.ParseFrameLine(strings.TrimSpace(decoy)); err != nil || frame.Type != "announce_routes" {
		t.Fatalf("fixture must parse as announce_routes; got %q, err %v", frame.Type, err)
	}

	writeWireLine(t, peerEnd, decoy)
	requireNoInboxFrame(t, session, "an oversize announce frame that showed the pre-parse scan an entitled type")

	// And the loop is still alive for a legal frame.
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusal = %q, want ping", frame.Type)
	}
}

// TestPreParseFrameLineAdmissionHasThreeAnswers states the admission rule on
// the function itself, so every branch of it is pinned in one place rather than
// only where a fixture happens to exercise it.
//
// The three verdicts are three different findings:
//
//   - AMBIGUOUS is refused at ANY size. A line whose type only
//     protocol.ParseFrameLine could resolve cannot be routed before the parse,
//     so it must not be parsed at all (§4.1 step 1). This is what removes the
//     residue branch rather than shrinking it;
//   - OVER BUDGET is a line past MaxFrameLine that did not name a type entitled
//     to the wide response budget — including every type this node has never
//     heard of, which is the closure finding 2 is about;
//   - ADMIT is everything else: inside MaxFrameLine whatever it names, and past
//     it only for a named member of hasWideFrameLineBudget.
func TestPreParseFrameLineAdmissionHasThreeAnswers(t *testing.T) {
	t.Parallel()

	within := strings.Repeat("x", protocol.MaxFrameLine-1)
	pad := func(claimed string) string {
		return `{"type":"` + claimed + `","pad":"` + strings.Repeat("x", protocol.MaxFrameLine) + `"}`
	}

	cases := map[string]struct {
		line string
		want preParseFrameLineVerdict
	}{
		"inside the budget, unnamed":     {line: within, want: preParseAdmit},
		"inside the budget, strict type": {line: `{"type":"datagram","pad":"` + within[:1024] + `"}`, want: preParseAdmit},
		"inside the budget, ambiguous": {
			line: `{"type":"contacts","type":"datagram"}`,
			want: preParseRefuseAmbiguous,
		},
		"past the budget, unnamed":             {line: within + "x", want: preParseRefuseOverBudget},
		"past the budget, strict type":         {line: pad(protocol.DatagramFrameType), want: preParseRefuseOverBudget},
		"past the budget, announce-plane type": {line: pad("announce_routes"), want: preParseRefuseOverBudget},
		// The closure of finding 2: a type nobody enumerated does NOT inherit
		// the 8 MiB response budget by being unrecognised.
		"past the budget, unknown type":          {line: pad("some_future_reply_v9"), want: preParseRefuseOverBudget},
		"past the budget, reply with no handler": {line: pad("messages"), want: preParseRefuseOverBudget},
		"past the budget, entitled contacts":     {line: pad("contacts"), want: preParseAdmit},
		"past the budget, entitled push_message": {line: pad("push_message"), want: preParseAdmit},
		"past the budget, type named twice": {
			line: `{"type":"contacts","pad":"` + strings.Repeat("x", protocol.MaxFrameLine) +
				`","type":"datagram"}`,
			want: preParseRefuseAmbiguous,
		},
		"past the budget, type only nested": {
			line: `{"item":{"type":"contacts"},"pad":"` + strings.Repeat("x", protocol.MaxFrameLine) + `"}`,
			want: preParseRefuseOverBudget,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, got := admitFrameLinePreParse(tc.line); got != tc.want {
				t.Fatalf("admitFrameLinePreParse(%d bytes) = %v, want %v",
					wireLineBudget(tc.line), got, tc.want)
			}
		})
	}
}

// TestStrictFrameLineBudgetCountsTheNewline pins the accounting unit of §2.3
// on the shared predicate: the sender caps the frame INCLUDING the newline it
// writes, so a receiver counting without it accepts one byte the writer would
// have refused.
func TestStrictFrameLineBudgetCountsTheNewline(t *testing.T) {
	t.Parallel()

	// A line whose bytes plus the newline are exactly MaxFrameLine passes.
	exact := strings.Repeat("x", protocol.MaxFrameLine-1)
	if exceedsStrictFrameLineBudget(protocol.DatagramFrameType, exact) {
		t.Fatalf("a line of exactly MaxFrameLine including the newline was refused; budget = %d", wireLineBudget(exact))
	}
	if !exceedsStrictFrameLineBudget(protocol.DatagramFrameType, exact+"x") {
		t.Fatal("one byte past MaxFrameLine must be refused")
	}
	// The same line as it really arrives — with the newline present — must
	// give the same verdict, or the two receive paths disagree by one byte.
	if exceedsStrictFrameLineBudget(protocol.DatagramFrameType, exact+"\n") {
		t.Fatal("the newline was counted twice")
	}
	// The allowlisted batching replies keep the wide response-plane budget.
	if exceedsStrictFrameLineBudget("contacts", exact+"x") {
		t.Fatal("a contacts reply was narrowed to the 128 KiB budget")
	}
	// And a type outside the allowlist does not inherit it.
	if !exceedsStrictFrameLineBudget("some_future_reply_v9", exact+"x") {
		t.Fatal("an unenumerated type kept the 8 MiB budget: the entitled set is not closed")
	}
}

// TestInboundOversizeDatagramIsASilentDropThatStillPays is the inbound half of
// the §2.3 price rule: the same verdict the peer-session reader reaches, on the
// reader that used to answer an over-long line by closing the connection.
//
// A neighbour that relays a frame did not write it, so the size of the line it
// hands over is not misbehaviour it can be charged for (§4.4) — and on this
// direction "charged" meant the strongest answer the node has: an error frame
// and a tear-down. The refusal is now silent, the remainder is skipped so the
// connection resynchronises on the next frame, and what the line costs is the
// bytes it made this node read, billed to the plane's own §5 budget.
//
// The controls are the whole safety of the rule: the claim buys the silence for
// NOTHING but a datagram, and only where there is a budget to charge and a
// neighbour to charge it to.
func TestInboundOversizeDatagramIsASilentDropThatStillPays(t *testing.T) {
	oversize := oversizeUnparseableDatagramLine(t)

	t.Run("claimed_datagram_from_a_billable_neighbour", func(t *testing.T) {
		svc, _, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		reader := newTestBufioReader(oversize + `{"type":"ping"}` + "\n")

		line, err := svc.readInboundCommandLine(reader, connID)
		if !errors.Is(err, errInboundLineDropped) {
			t.Fatalf("read = (%.32q, %v), want the drop sentinel: an oversize datagram must not end the connection", line, err)
		}
		if got := datagramDropCount(svc, datagram.DropFrameTooLarge); got != 1 {
			t.Fatalf("frame_too_large drops = %d, want 1: the limit itself must still refuse the line", got)
		}
		// EXACTLY the line, not "at least the limit": the fixture ends past the
		// limit inside the chunk that crossed it, so a charge reconstructed from
		// MaxFrameLine plus the discard hands those last bytes over for free —
		// and on this path the bytes are the entire price of the refusal.
		if charged := datagramAdmissionStats(svc).AdmittedBytes; charged != uint64(len(oversize)) {
			t.Fatalf("the §5 budget was charged %d bytes for a line of %d: an under-charged refusal is a load channel priced below cost",
				charged, len(oversize))
		}
		if score := banScoreForIP(svc, datagramTestPeerIP); score != 0 {
			t.Fatalf("ban score = %d for an oversize datagram (§4.4)", score)
		}

		// The remainder was skipped, so the connection is still framed: the
		// next line is read whole rather than as the tail of the last one.
		next, err := svc.readInboundCommandLine(reader, connID)
		if err != nil {
			t.Fatalf("the reader did not resynchronise after the refusal: %v", err)
		}
		if strings.TrimSpace(next) != `{"type":"ping"}` {
			t.Fatalf("next line = %.32q, want the ping that followed the oversize line", next)
		}
	})

	t.Run("a_stream_rather_than_a_frame_still_ends_the_connection", func(t *testing.T) {
		svc, _, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		// Never terminated, and far past the one frame of slack §2.3 can
		// justify: at that point the peer is streaming, not framing, and the
		// silent rule stops applying — otherwise the claim would buy an
		// unbounded read.
		stream := `{"type":"datagram","payload":"` + strings.Repeat("A", 3*protocol.MaxFrameLine)

		if _, err := svc.readInboundCommandLine(newTestBufioReader(stream), connID); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("read error = %v, want errFrameTooLarge: the resynchronisation window is %d bytes, not unbounded",
				err, oversizeDatagramResyncBytes)
		}
	})

	t.Run("another_plane_still_ends_the_connection", func(t *testing.T) {
		svc, _, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		other := oversizeLineOfType(t, "some_future_reply_v9", protocol.MaxFrameLine)

		if _, err := svc.readInboundCommandLine(newTestBufioReader(other), connID); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("read error = %v, want errFrameTooLarge: only the datagram plane carries the silent rule", err)
		}
	})

	t.Run("an_unbillable_neighbour_buys_nothing", func(t *testing.T) {
		svc, _, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		// No proven identity, hence no key the charge could land on: the claim
		// must not buy a refusal nobody pays for.
		svc.clearConnAuth(connID)

		if _, err := svc.readInboundCommandLine(newTestBufioReader(oversize), connID); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("read error = %v, want errFrameTooLarge for a neighbour with no billable key", err)
		}
	})

	t.Run("no_layer_buys_nothing", func(t *testing.T) {
		// No conveyor, so no §5 budget to move the cost onto — the same
		// "no layer, no exemption" condition the command-limiter carve-out has.
		svc, _, connID := newDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		if svc.datagramLayer() != nil {
			t.Fatal("precondition: this fixture must have no conveyor")
		}
		if _, err := svc.readInboundCommandLine(newTestBufioReader(oversize), connID); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("read error = %v, want errFrameTooLarge with no layer to charge", err)
		}
	})
}

// TestInboundCommandReaderIsTheStrictBudget pins the inbound half of the
// symmetry.
//
// There is no pre-parse gate to hoist on that path, and the reason is this
// equality: the inbound command reader's own limit IS MaxFrameLine, so an
// oversize line never reaches dispatch — it is refused by the reader itself,
// which then either ends the connection or, for the one plane whose size rule is
// silent, skips the line and reads on (readInboundCommandLine). Widening
// maxCommandLineBytes without adding a gate would silently reopen on inbound
// exactly the hole §2.3 closes on the peer-session reader, and this test is what
// stops that from being quiet.
func TestInboundCommandReaderIsTheStrictBudget(t *testing.T) {
	t.Parallel()

	if maxCommandLineBytes != protocol.MaxFrameLine {
		t.Fatalf("maxCommandLineBytes = %d, MaxFrameLine = %d: the inbound path no longer enforces the strict budget by its reader alone, so it needs an explicit pre-parse gate",
			maxCommandLineBytes, protocol.MaxFrameLine)
	}
	if _, err := readFrameLine(newTestBufioReader(oversizeUnparseableDatagramLine(t)), maxCommandLineBytes); err == nil {
		t.Fatal("the inbound command reader accepted a line past MaxFrameLine")
	}
	// The peer-session reader really is the wide one — that asymmetry is why
	// the gate above has to exist at all.
	if maxResponseLineBytes <= protocol.MaxFrameLine {
		t.Fatal("precondition: the peer-session reader must be wider than the strict budget")
	}
}

// TestInboundReadLoopHandsOverTheWireLine pins the one step of the inbound
// chain no unit test can reach: handleConn is entered with a live socket it
// registers itself, so the call it makes into handleCommand cannot be observed
// without standing up a whole connection.
//
// What matters about that call is a single property — the argument is the line
// as read, not a trimmed copy — and the property is structural, so it is
// asserted structurally, the same way command_scope_test asserts the dispatch
// switch. Re-introducing strings.TrimSpace there is silent otherwise: every
// frame still works, and only the datagram byte budget quietly starts counting
// fewer bytes than the wire carried (§5).
func TestInboundReadLoopHandsOverTheWireLine(t *testing.T) {
	t.Parallel()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(filepath.Dir(thisFile), "service.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse service.go: %v", err)
	}

	calls := 0
	var insideHandleConn bool
	ast.Inspect(file, func(n ast.Node) bool {
		if fn, isFunc := n.(*ast.FuncDecl); isFunc {
			insideHandleConn = fn.Name.Name == "handleConn"
			return true
		}
		if !insideHandleConn {
			return true
		}
		call, isCall := n.(*ast.CallExpr)
		if !isCall {
			return true
		}
		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "handleCommand" || len(call.Args) != 2 {
			return true
		}
		calls++
		argument, isIdent := call.Args[1].(*ast.Ident)
		if !isIdent || argument.Name != "line" {
			t.Errorf("handleConn passes %T to handleCommand instead of the wire line: the datagram byte budget of §5 is charged on those bytes", call.Args[1])
		}
		return true
	})
	if calls != 1 {
		t.Fatalf("found %d handleCommand calls in handleConn, want exactly 1", calls)
	}
}

// ---------------------------------------------------------------------------
// §3.4 / §5 — the plane counts and parses the bytes the wire carried
// ---------------------------------------------------------------------------

// paddedDatagramWireLine returns a VALID datagram line padded with outside
// whitespace, plus the trimmed form, so a test can tell which of the two the
// layer was given.
//
// The padding is legal on the wire and invisible to any JSON parser, which is
// exactly what makes it the right probe: nothing downstream behaves
// differently because of it EXCEPT the byte budget, and the budget is the
// thing being tested.
func paddedDatagramWireLine(t *testing.T) (wire, trimmed string) {
	t.Helper()
	line := mustDatagramLine(t, newNodeDatagram(t, nil))
	trimmed = strings.TrimSpace(line)
	wire = strings.Repeat(" ", 4096) + trimmed + strings.Repeat(" ", 4096) + "\n"
	if len(wire) <= len(trimmed) {
		t.Fatal("fixture did not pad")
	}
	if wireLineBudget(wire) > protocol.MaxFrameLine {
		t.Fatal("the padded fixture must stay inside the strict budget")
	}
	return wire, trimmed
}

// TestOutboundSessionChargesTheDatagramBudgetOnTheWireLine is the §5 half of
// finding 1 on the peer-session path.
//
// The admission budget of §5 is spent on the wire, so it has to be counted on
// the wire. With the trimmed line reaching the layer, a peer pads a frame with
// outside whitespace to just under MaxFrameLine and has almost none of it
// charged — the byte budget stops bounding what the neighbour can push. The
// assertion is on AdmittedBytes, which HandleInbound charges from len(Line)
// before anything is decoded (§4.1 step 1).
func TestOutboundSessionChargesTheDatagramBudgetOnTheWireLine(t *testing.T) {
	t.Parallel()

	svc, peerEnd, _ := newReadPeerSessionFixture(t)
	wire, trimmed := paddedDatagramWireLine(t)

	writeWireLine(t, peerEnd, wire)
	waitForAdmittedFrames(t, svc, 1)

	charged := svc.datagramLayer().admission.Stats().AdmittedBytes
	if charged != uint64(len(wire)) {
		t.Fatalf("charged %d bytes, want %d — the wire line, not the %d-byte trimmed form",
			charged, len(wire), len(trimmed))
	}
}

// TestInboundDispatchChargesTheDatagramBudgetOnTheWireLine is the same
// obligation on the other direction. The two paths must agree, or a peer picks
// the cheaper one.
func TestInboundDispatchChargesTheDatagramBudgetOnTheWireLine(t *testing.T) {
	t.Parallel()

	svc, _, connID := newLayeredDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	wire, trimmed := paddedDatagramWireLine(t)

	if !svc.handleCommand(connID, wire) {
		t.Fatal("a padded datagram tore the inbound connection down")
	}
	charged := svc.datagramLayer().admission.Stats().AdmittedBytes
	if charged != uint64(len(wire)) {
		t.Fatalf("charged %d bytes, want %d — the wire line, not the %d-byte trimmed form",
			charged, len(wire), len(trimmed))
	}
}

// TestOutboundSessionKeepsTheDatagramRawLineIntact pins the §3.4 half: the
// line the reader hands the strict parser is the one the wire delivered, byte
// for byte, and no datagram field is lost on the way.
//
// The observable is the conveyor rather than a Frame in the inbox, because the
// reader no longer builds one: §4.1 step 1 puts the neighbour's budget before
// any decoding, so a datagram is diverted to the ingress ahead of
// protocol.ParseFrameLine. What that leaves to check is the same statement from
// the other end — the strict parser accepted the padded WIRE line, and the
// whole of it was charged.
func TestOutboundSessionKeepsTheDatagramRawLineIntact(t *testing.T) {
	t.Parallel()

	svc, peerEnd, _ := newReadPeerSessionFixture(t)
	wire, trimmed := paddedDatagramWireLine(t)

	// The fixture is only a probe if the strict parser really accepts the
	// padded form: otherwise "not malformed" below would prove nothing.
	parsed, err := protocol.ParseDatagramFrameLine(wire)
	if err != nil {
		t.Fatalf("the strict parser refused the padded wire line: %v", err)
	}
	original := newNodeDatagram(t, nil)
	if parsed.DType != original.DType || parsed.Mode != original.Mode || parsed.Class != original.Class {
		t.Fatalf("header fields lost: %+v", parsed)
	}
	if parsed.Auth == nil || parsed.RoutePolicy != original.RoutePolicy {
		t.Fatalf("optional blocks lost: route_policy=%q auth=%v", parsed.RoutePolicy, parsed.Auth)
	}

	writeWireLine(t, peerEnd, wire)
	waitForAdmittedFrames(t, svc, 1)
	waitForObservedFrames(t, svc, 1)

	if got := datagramDropCount(svc, datagram.DropMalformed); got != 0 {
		t.Fatalf("the strict parser called the wire line malformed %d times: it was handed something the wire never carried", got)
	}
	if charged := svc.datagramLayer().admission.Stats().AdmittedBytes; charged != uint64(len(wire)) {
		t.Fatalf("charged %d bytes, want %d — the wire line, not the %d-byte trimmed form",
			charged, len(wire), len(trimmed))
	}
}

// TestAnnouncePlaneKeepsTheTrimmedRawLine pins the OTHER side of the split
// rawLineForDispatch makes.
//
// The announce-plane types stay on the trimmed line deliberately: their
// dispatch is a whitespace-agnostic Unmarshal, they carry no per-neighbour
// byte budget the choice could distort, and trimmed is what the inbound TCP
// dispatcher already hands them — so keeping it is what holds the two
// directions identical for every type finding 1 had no reason to move.
func TestAnnouncePlaneKeepsTheTrimmedRawLine(t *testing.T) {
	t.Parallel()

	wire := "  " + `{"type":"route_sync_digest_v1"}` + "  \n"
	trimmed := strings.TrimSpace(wire)

	if got := rawLineForDispatch(protocol.DatagramFrameType, wire, trimmed); got != wire {
		t.Fatalf("a datagram was handed %q, want the wire line", got)
	}
	for _, frameType := range []string{
		"route_sync_digest_v1",
		"route_sync_summary_v1",
		protocol.RouteAnnounceV3FrameType,
		protocol.RoutePoisonFrameType,
		protocol.RoutePoisonV2FrameType,
	} {
		if got := rawLineForDispatch(frameType, wire, trimmed); got != trimmed {
			t.Errorf("rawLineForDispatch(%q) = %q, want the trimmed line", frameType, got)
		}
	}
}
