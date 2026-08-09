package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_preparse_classification_test.go pins the two rules that make §4.1
// step 1 (spec line 417 — "admission by bytes and frames, BEFORE any decoding")
// literally true on both receive paths:
//
//   - a line whose type cannot be resolved without parsing it is refused
//     WITHOUT parsing it. Before this, such a line went through
//     protocol.ParseFrameLine and was dispatched on the type the PARSER
//     resolved, so a duplicate `type` key ending in `datagram` reached the
//     layer only after a full universal unmarshal nobody had charged for;
//   - the 8 MiB response budget is bought by an ENUMERATED type, never
//     inherited by being unrecognised. An authenticated peer naming a type this
//     build has never heard of used to get multi-megabyte JSON decoded on
//     demand, once per frame.

// ---------------------------------------------------------------------------
// The classification itself
// ---------------------------------------------------------------------------

// TestClassifyFrameLineSeparatesAmbiguousFromUnnamed pins the three-way answer.
//
// The split matters because the two refusals are different decisions: an
// AMBIGUOUS line must never reach the parser, while an UNNAMED one may — the
// parser cannot get a dispatchable type out of it either, so letting it through
// costs a bounded decode and preserves the invalid_json / unknown_command
// answers the command plane owes a client.
func TestClassifyFrameLineSeparatesAmbiguousFromUnnamed(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		line  string
		want  string
		class frameLineClass
	}{
		"plain": {
			line: `{"type":"messages","count":1}`, want: "messages", class: frameLineNamed,
		},
		"not the first key": {
			line: `{"count":1,"type":"messages"}`, want: "messages", class: frameLineNamed,
		},
		"nested type seen first": {
			line: `{"item":{"type":"messages"},"type":"datagram"}`, want: "datagram", class: frameLineNamed,
		},

		// Ambiguous: the parser and the scan can name two different types.
		"duplicate top-level type": {
			line: `{"type":"messages","pad":"x","type":"datagram"}`, class: frameLineAmbiguous,
		},
		"escaped key": {
			// encoding/json decodes \u0070 to `p` and reads the key as `type`.
			line: `{"ty\u0070e":"datagram","count":1}`, class: frameLineAmbiguous,
		},
		"escaped value": {
			line: `{"type":"\u0064atagram"}`, class: frameLineAmbiguous,
		},
		"case-variant key": {
			// encoding/json accepts a case-insensitive match, so the parser
			// reads a datagram off a key a byte comparison calls unrelated.
			line: `{"TYPE":"datagram"}`, class: frameLineAmbiguous,
		},
		"null then a real type": {
			// JSON null leaves a string field untouched, so the parser reads
			// `datagram` from a line whose first candidate named nothing.
			line: `{"type":null,"type":"datagram"}`, class: frameLineAmbiguous,
		},
		"type is not a string": {
			line: `{"type":7}`, class: frameLineAmbiguous,
		},

		// Unnamed: the parser cannot produce a dispatchable type either.
		"only a nested type": {
			line: `{"item":{"type":"messages"}}`, class: frameLineUnnamed,
		},
		"no type at all": {
			line: `{"count":1}`, class: frameLineUnnamed,
		},
		"not an object": {
			line: `["type","datagram"]`, class: frameLineUnnamed,
		},
		"unterminated object": {
			line: `{"type":"datagram"`, class: frameLineUnnamed,
		},
		"a value that merely contains the key text": {
			line: `{"note":"\"type\":\"datagram\"","type":"messages"}`, want: "messages", class: frameLineNamed,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, class := classifyFrameLine(tc.line)
			if class != tc.class {
				t.Fatalf("classifyFrameLine(%s) class = %v, want %v (value %q)", tc.line, class, tc.class, got)
			}
			if class == frameLineNamed && got != tc.want {
				t.Fatalf("classifyFrameLine(%s) = %q, want %q", tc.line, got, tc.want)
			}
		})
	}
}

// TestNoUnnamedLineParsesAsADispatchableType is the ORACLE the residue removal
// rests on.
//
// Removing the `datagram` delivery from below protocol.ParseFrameLine is only
// sound if the two readers cannot disagree on a line neither refused, and the
// argument has one direction that has to hold for every input: whatever
// classifyFrameLine calls frameLineUnnamed, the parser must NOT resolve to a
// non-empty type. If it ever does, that type is dispatched from the switch
// while the classification sent it down the ordinary path — which is precisely
// the blind spot the residue branch used to paper over.
//
// The frameLineNamed direction is checked too: the scan's answer must be the
// parser's answer, byte for byte.
func TestNoUnnamedLineParsesAsADispatchableType(t *testing.T) {
	t.Parallel()

	corpus := []string{
		`{"type":"datagram"}`,
		`{"type":"ping"}`,
		`{"TYPE":"datagram"}`,
		`{"Type":"datagram"}`,
		`{"tYpE":"datagram"}`,
		`{"type":"datagram"}`,
		`{"ty\u0070e":"datagram"}`,
		`{"type":"\u0064atagram"}`,
		`{"type":null,"type":"datagram"}`,
		`{"type":null}`,
		`{"type":"messages","type":"datagram"}`,
		`{"type":"datagram","type":"messages"}`,
		`{"item":{"type":"datagram"}}`,
		`{"item":{"type":"datagram"},"type":"ping"}`,
		`{"items":[{"type":"datagram"}]}`,
		`{"note":"\"type\":\"datagram\""}`,
		`{"count":1}`,
		`{}`,
		`   {"type":"datagram"}   `,
		`{"type" : "datagram"}`,
		`["type","datagram"]`,
		`{"type":"datagram"`,
		`{"type":7}`,
		`{"type":["datagram"]}`,
		`{"type":{"a":"datagram"}}`,
		`{"a":"type","type":"datagram"}`,
		`{"a":{"b":[1,2]},"type":"datagram"}`,
	}

	for _, line := range corpus {
		claimed, class := classifyFrameLine(line)
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
		parsed := ""
		if err == nil {
			parsed = frame.Type
		}
		switch class {
		case frameLineNamed:
			if err == nil && parsed != claimed {
				t.Errorf("classifyFrameLine(%s) = %q but the parser says %q: a named line is dispatched on the scan's answer",
					line, claimed, parsed)
			}
		case frameLineUnnamed:
			if parsed != "" {
				t.Errorf("classifyFrameLine(%s) called the line unnamed while the parser resolved it to %q: that type would be dispatched below the parser, uncharged",
					line, parsed)
			}
		case frameLineAmbiguous:
			// Refused before the parse; the parser's answer is irrelevant.
		}
	}
}

// TestNoDatagramDeliveryBelowTheUniversalParser is the structural half of the
// same statement, and the mutation killer for "put the residue branch back".
//
// The `datagram` case survives in both dispatch switches as a declaration and
// an assertion — the AST protocol oracle in command_scope_test reads it, and a
// frame arriving there proves the two scanners disagree — but it must not
// DELIVER. A delivery from inside either switch would put protocol.ParseFrameLine
// back in front of the neighbour's budget, which is finding 1 exactly.
func TestNoDatagramDeliveryBelowTheUniversalParser(t *testing.T) {
	t.Parallel()

	forbidden := map[string]bool{
		"dispatchSessionDatagramLine": true,
		"dispatchInboundDatagramLine": true,
		"dispatchInboundDatagramWire": true,
		"handleDatagramFrame":         true,
	}
	for _, where := range []struct {
		file     string
		function string
	}{
		{"service.go", "dispatchNetworkFrame"},
		{"peer_management.go", "dispatchPeerSessionFrame"},
	} {
		for _, called := range callsInsideCaseClause(t, where.file, where.function, protocol.DatagramFrameType) {
			if forbidden[called] {
				t.Errorf("the `datagram` case of %s.%s calls %s: a datagram delivered from a dispatch switch has already been through the universal parser, which §4.1 step 1 forbids before admission",
					where.file, where.function, called)
			}
		}
	}
}

// callsInsideCaseClause returns the names of every method/function called from
// inside ONE case clause of the switch statements in one named function.
//
// The scope is the clause and not the whole function on purpose: the diversion
// this rule protects calls the very same helpers, one step ABOVE the parser,
// which is where they belong.
func callsInsideCaseClause(t *testing.T, file, function, label string) []string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Join(filepath.Dir(thisFile), file), nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	var names []string
	clauses := 0
	for _, declaration := range parsed.Decls {
		fn, isFunc := declaration.(*ast.FuncDecl)
		if !isFunc || fn.Name.Name != function {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			clause, isClause := n.(*ast.CaseClause)
			if !isClause || !caseClauseNames(clause, label) {
				return true
			}
			clauses++
			ast.Inspect(clause, func(inner ast.Node) bool {
				call, isCall := inner.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					names = append(names, fun.Sel.Name)
				case *ast.Ident:
					names = append(names, fun.Name)
				}
				return true
			})
			return true
		})
	}
	if clauses != 1 {
		t.Fatalf("found %d `%s` case clauses in %s.%s, want exactly 1: the declaration the protocol oracle reads is gone",
			clauses, label, file, function)
	}
	return names
}

// caseClauseNames reports whether a case clause carries the given string
// literal as one of its labels.
func caseClauseNames(clause *ast.CaseClause, label string) bool {
	for _, expr := range clause.List {
		lit, isLit := expr.(*ast.BasicLit)
		if isLit && lit.Kind == token.STRING && strings.Trim(lit.Value, `"`) == label {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Finding 1 — the ambiguous line never reaches protocol.ParseFrameLine
// ---------------------------------------------------------------------------

// ambiguousDatagramDecoy is the discriminating fixture of this file: a line
// whose LAST top-level `type` is `datagram` — so encoding/json calls it one —
// and which protocol.ParseFrameLine nevertheless REFUSES.
//
// The refusal is what separates the two orders. With the classification first
// the line is refused unparsed and the neighbour is charged for the bytes it
// made this node scan. With protocol.ParseFrameLine first the line dies on the
// parser's own error, the residue branch is never reached and the neighbour
// pays nothing at all — the free decode §4.1 step 1 exists to prevent.
func ambiguousDatagramDecoy(t *testing.T) string {
	t.Helper()
	// `capabilities` is a []string on the universal Frame, so a number there is
	// a decode error; the duplicate `type` is what makes the two readers
	// disagree in the first place.
	line := `{"type":"contacts","capabilities":7,"type":"datagram"}` + "\n"
	if peeked := peekFrameType(line); peeked != "contacts" {
		t.Fatalf("fixture is not a decoy: peekFrameType = %q", peeked)
	}
	if _, class := classifyFrameLine(line); class != frameLineAmbiguous {
		t.Fatalf("fixture is not ambiguous: class = %v", class)
	}
	if _, err := protocol.ParseFrameLine(strings.TrimSpace(line)); err == nil {
		t.Fatal("fixture parses: it cannot distinguish a refusal before the parse from one after it")
	}
	if wireLineBudget(line) > protocol.MaxFrameLine {
		t.Fatal("fixture must be inside the strict budget, or the BUDGET half would refuse it instead")
	}
	return line
}

// TestOutboundSessionRefusesAnAmbiguousLineBeforeTheParser is finding 1 on the
// peer-session reader, which is the direction that matters most: it accepts up
// to 8 MiB and has no general command limiter in front of it.
//
// The mutation this kills: dropping the preParseRefuseAmbiguous branch from
// refuseUnadmissibleFrameLine. The line then dies inside protocol.ParseFrameLine
// and the neighbour is charged nothing.
func TestOutboundSessionRefusesAnAmbiguousLineBeforeTheParser(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newReadPeerSessionFixture(t)
	writeWireLine(t, peerEnd, ambiguousDatagramDecoy(t))

	// Charged: §4.1 step 1 bills the neighbour before anything is decoded, and
	// an unmetered refusal would be this reader's only free one.
	waitForAdmittedFrames(t, svc, 1)
	requireNoInboxFrame(t, session, "a line whose type only the parser could resolve")

	// And the CONVEYOR never saw it: the refusal is above the layer, not inside
	// it, so the strict parser was not paid for either.
	if got := datagramObservedCount(svc); got != 0 {
		t.Fatalf("the conveyor decided on %d frames from a line refused before the parse", got)
	}

	// The session survives — this is a drop, never a tear-down.
	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusal = %q, want ping", frame.Type)
	}
}

// TestInboundDispatchRefusesAnAmbiguousLineBeforeTheParser is the same finding
// on the accepted-connection path. The two directions have to agree, or a peer
// picks the cheaper one.
//
// The mutation this kills: removing the preParseRefuseAmbiguous check from
// dispatchNetworkFrame. The line then reaches protocol.ParseFrameLine, fails
// there, and the connection is torn down with invalid_json — a verdict this
// node deliberately never reached — while the neighbour is charged nothing.
func TestInboundDispatchRefusesAnAmbiguousLineBeforeTheParser(t *testing.T) {
	svc, _, connID := newLayeredDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	line := ambiguousDatagramDecoy(t)

	if !svc.handleCommand(connID, line) {
		t.Fatal("an ambiguous line tore the inbound connection down: nothing on the wire proves what the sender meant")
	}
	stats := svc.datagramLayer().admission.Stats()
	if stats.Admitted != 1 {
		t.Fatalf("the neighbour was charged for %d frames, want 1: the universal parser swallowed the line before admission", stats.Admitted)
	}
	if stats.AdmittedBytes != uint64(len(line)) {
		t.Fatalf("charged %d bytes, want the whole wire line (%d)", stats.AdmittedBytes, len(line))
	}
	if got := datagramObservedCount(svc); got != 0 {
		t.Fatalf("the conveyor decided on %d frames from a line refused before the parse", got)
	}
}

// TestAmbiguousDatagramClaimStillReachesTheLedger pins the §10 accounting of
// the new refusal: a line the best-effort peek calls a datagram lands on
// DropMalformed, which is the verdict the strict parser of §3.4 reached for the
// same shape one step later, before the refusal moved above the parser.
func TestAmbiguousDatagramClaimStillReachesTheLedger(t *testing.T) {
	t.Parallel()

	svc, peerEnd, _ := newReadPeerSessionFixture(t)
	// Peeks as `datagram`, parses as `ping`: the attribution is the peek's.
	writeWireLine(t, peerEnd, `{"type":"datagram","type":"ping"}`+"\n")

	waitForCondition(t, 5*time.Second, func() bool {
		return datagramDropCount(svc, datagram.DropMalformed) == 1
	})
}

// TestLegalFramesSurviveTheAmbiguityRefusal is the negative control the
// refusal needs: every shape a real peer puts on the wire still gets through.
//
// It matters because the refusal is unconditional on size, so a classifier that
// was even slightly too eager would silently cut working traffic rather than
// produce an obvious failure.
func TestLegalFramesSurviveTheAmbiguityRefusal(t *testing.T) {
	t.Parallel()

	legal := []protocol.Frame{
		{Type: "ping"},
		{Type: "pong"},
		{Type: "welcome", Challenge: "abc", Capabilities: []string{"mesh_routing_v1"}},
		{Type: "auth_ok"},
		{Type: "peers", Count: 1, Peers: []string{"10.0.0.1:9000"}},
		{Type: "contacts", Count: 1, Contacts: []protocol.ContactFrame{{Address: "a", PubKey: "b"}}},
		{Type: "messages", Topic: "dm", Count: 0},
		{Type: "inbox", Topic: "dm", Recipient: "r"},
		{Type: "announce_routes"},
		{Type: "routes_update"},
		{Type: "request_resync"},
		{Type: "relay_message", Body: "x"},
		{Type: "relay_hop_ack"},
		{Type: "push_notice", Ciphertext: "x"},
		{Type: "announce_peer"},
		{Type: "ack_delete"},
		{Type: "error", Code: "rate-limited"},
	}
	for _, frame := range legal {
		line, err := protocol.MarshalFrameLine(frame)
		if err != nil {
			t.Fatalf("MarshalFrameLine(%s): %v", frame.Type, err)
		}
		claimed, verdict := admitFrameLinePreParse(line)
		if verdict != preParseAdmit {
			t.Errorf("a marshalled %s frame was refused pre-parse (%v)", frame.Type, verdict)
		}
		if claimed != frame.Type {
			t.Errorf("admission read %q off a marshalled %s frame", claimed, frame.Type)
		}
	}

	// The raw-line frame types build their own JSON and must survive the same
	// scan, because the reader classifies the bytes, not the builder.
	for _, raw := range []string{
		`{"type":"route_sync_digest_v1","digest":"ab","entries":[{"i":"aa","s":1}]}`,
		`{"type":"route_sync_summary_v1","digest":"ab","match":true,"expect_full_sync":false}`,
		`{"type":"route_announce_v3","kind":"delta","entries":[]}`,
		`{"type":"route_poison_v1","identity":"aa"}`,
		`{"type":"route_poison_v2","identities":["aa","bb"]}`,
		`{"type":"file_command","src":"aa","dst":"bb","payload":"Zm9v"}`,
	} {
		if _, verdict := admitFrameLinePreParse(raw + "\n"); verdict != preParseAdmit {
			t.Errorf("a raw-line frame was refused pre-parse: %s", raw)
		}
	}
}

// ---------------------------------------------------------------------------
// Finding 2 — the wide budget is bought, never inherited
// ---------------------------------------------------------------------------

// TestUnknownOversizeTypeIsRefusedBeforeParsing is finding 2 on the production
// reader: an authenticated peer naming a type this build has never heard of
// used to get up to 8 MiB of JSON decoded per frame, on demand, for as long as
// it cared to keep sending.
//
// The mutation this kills: restating the entitlement as the complement of the
// strict set (`return isStrictFrameLineBudgetType(claimed)`), under which
// `some_future_reply_v9` is admitted and the line lands in the inbox.
func TestUnknownOversizeTypeIsRefusedBeforeParsing(t *testing.T) {
	t.Parallel()

	_, peerEnd, session := newReadPeerSessionFixture(t)

	oversize := `{"type":"some_future_reply_v9","pad":"` +
		strings.Repeat("u", protocol.MaxFrameLine) + `"}` + "\n"
	if wireLineBudget(oversize) <= protocol.MaxFrameLine {
		t.Fatal("fixture must exceed MaxFrameLine")
	}
	if _, named := topLevelFrameType(oversize); !named {
		t.Fatal("fixture must name its type unambiguously, or it would be refused for the OTHER reason")
	}

	writeWireLine(t, peerEnd, oversize)
	requireNoInboxFrame(t, session, "an oversize line of a type nobody enumerated")

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("frame after the refusal = %q, want ping", frame.Type)
	}
}

// TestEveryWideBudgetTypePassesOversize is the other half, and it is what stops
// the allowlist from being tightened into a regression: EVERY member has to get
// an oversize line through the production reader, or a legitimate batched reply
// starts being dropped.
//
// The fixtures are shaped like the real frames rather than padded blobs, so a
// member that only passes because the test padded a string it does not have
// would fail here instead of in the field.
func TestEveryWideBudgetTypePassesOversize(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		frame protocol.Frame
	}{
		{
			// contactsFrame() has no count cap; ~400 contacts pass 128 KiB.
			name:  "contacts",
			frame: oversizeContactsFrame(),
		},
		{
			// dispatchPeerSessionFrame accepts Item.Body up to
			// maxPeerCommandBodyBytes, which IS MaxFrameLine, so the largest
			// ACCEPTED push_message is necessarily an oversize line.
			name: "push_message",
			frame: protocol.Frame{
				Type:      "push_message",
				Topic:     "dm",
				Recipient: "r",
				Item: &protocol.MessageFrame{
					ID:     "m1",
					Sender: "s",
					Body:   strings.Repeat("B", maxPeerCommandBodyBytes),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			line, err := protocol.MarshalFrameLine(tc.frame)
			if err != nil {
				t.Fatalf("MarshalFrameLine: %v", err)
			}
			if wireLineBudget(line) <= protocol.MaxFrameLine {
				t.Fatalf("fixture is only %d bytes: it does not exercise the wide budget", wireLineBudget(line))
			}
			if wireLineBudget(line) > maxResponseLineBytes {
				t.Fatalf("fixture is %d bytes: past the reader's own limit", wireLineBudget(line))
			}
			if _, verdict := admitFrameLinePreParse(line); verdict != preParseAdmit {
				t.Fatalf("an oversize %s was refused pre-parse (%v): a legitimate batched reply is being dropped", tc.name, verdict)
			}

			// And through the real loop, so the assertion is about reception
			// rather than about a predicate.
			_, peerEnd, session := newReadPeerSessionFixture(t)
			if isSolicitedOnlyFrameType(tc.frame.Type) {
				// A reply only earns the wide budget while the request that
				// asked for it is outstanding — see grantFrameLineExtension.
				// The production caller is peerSessionRequest; here the state
				// is set directly because the test drives the reader, not the
				// request loop.
				defer session.admission.expectReply(tc.frame.Type)()
			}
			writeWireLine(t, peerEnd, line)
			if frame := awaitInboxFrame(t, session); frame.Type != tc.frame.Type {
				t.Fatalf("frame in the inbox = %q, want %q", frame.Type, tc.frame.Type)
			}
		})
	}
}

// oversizeContactsFrame builds a `contacts` reply the way contactsFrame() does
// — one entry per known address, no cap — large enough to pass MaxFrameLine.
func oversizeContactsFrame() protocol.Frame {
	const entries = 512
	contacts := make([]protocol.ContactFrame, entries)
	for i := range contacts {
		contacts[i] = protocol.ContactFrame{
			Address: strings.Repeat("a", 40),
			PubKey:  strings.Repeat("b", 64),
			BoxKey:  strings.Repeat("c", 64),
			BoxSig:  strings.Repeat("d", 128),
		}
	}
	return protocol.Frame{Type: "contacts", Count: len(contacts), Contacts: contacts}
}
