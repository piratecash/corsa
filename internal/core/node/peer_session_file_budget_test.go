package node

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// peer_session_file_budget_test.go covers the ONE carve-out the file data plane
// has out of the response-plane per-neighbour budget: an ADMISSIBLE
// `file_command` charges neither the shared raw bucket nor the violation ledger
// of peer_session_admission.go.
//
// The two halves are asserted separately because they fail differently:
//
//   - a stream of valid file commands must leave the shared bucket to the planes
//     that have no exemption, and must never end the session as `rate-limited`.
//     This is the live defect: a file transfer is the widest legitimate producer
//     on this reader (4 MiB/s is ~144 chunk frames per second) and the file
//     manager deliberately does not bound how many transfers run at once, so
//     ordinary use looked like abuse;
//   - the exemption is bought by ADMISSIBILITY, not by the claim in the line's
//     first bytes. A line that merely names the type, a line whose type the scan
//     and the parser could read differently, a line on a session that never
//     negotiated file_transfer_v1, and a line on a node with no file router: none
//     of them is a frame this reader will act on, so none of them may switch the
//     meter off.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// marshalFileCommandLine builds one wire line of the file plane's own frame
// format, padded through the field the plane really pads — the encrypted
// payload.
func marshalFileCommandLine(t *testing.T, payload string) string {
	t.Helper()
	raw, err := json.Marshal(protocol.FileCommandFrame{
		Type:    protocol.FileCommandFrameType,
		Nonce:   "nonce-peer-session-file-budget",
		TTL:     4,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("marshal file_command: %v", err)
	}
	return string(raw) + "\n"
}

// fileCommandLineOfSize returns a well-formed `file_command` line of EXACTLY
// size bytes, newline included.
//
// The exact size is what makes the budget assertions discriminating: "spent
// nothing" and "spent this line" are the two possible answers, and a fixture of
// unknown length can only distinguish them by inequality.
func fileCommandLineOfSize(t *testing.T, size int) string {
	t.Helper()
	envelope := marshalFileCommandLine(t, "")
	if size <= len(envelope) {
		t.Fatalf("a %d-byte file_command cannot carry the %d-byte envelope", size, len(envelope))
	}
	line := marshalFileCommandLine(t, strings.Repeat("A", size-len(envelope)))
	if len(line) != size {
		t.Fatalf("file_command fixture is %d bytes, want %d", len(line), size)
	}
	if wireLineBudget(line) > protocol.MaxFrameLine {
		t.Fatal("the fixture must stay inside the strict budget, or it is refused during the read instead of being charged")
	}
	return line
}

// ---------------------------------------------------------------------------
// A stream of valid file commands leaves the shared budget alone
// ---------------------------------------------------------------------------

// TestFileCommandStreamLeavesTheSharedBudgetToTheOtherPlanes is the live defect,
// stated on the bucket it was spending.
//
// The inbound direction has exempted `file_command` from its command limiter
// since the plane existed (exemptFrameTypeFromCommandLimit), because the file
// data plane legitimately runs far above any control-plane rate. The outbound
// peer-session reader exempted only `datagram`, so every chunk on a session this
// node dialled charged the shared 4 MiB/s bucket — and a bucket that runs out
// does not merely drop the chunk: it scores a violation, and five of them close
// the session with `rate-limited` plus a peer-health penalty.
//
// The POSITIVE CONTROL is the same session, the same frozen clock, the same
// bucket level, the same number of lines of the same SIZE, differing only in the
// type. Without it the test would also pass for an implementation that simply
// switched the response-plane budget off, which is the defence this budget is.
func TestFileCommandStreamLeavesTheSharedBudgetToTheOtherPlanes(t *testing.T) {
	t.Parallel()

	svc, peerEnd, session := newFileCommandReadFixture(t, domain.CapFileTransferV1)
	cache := installRecordingFileRouter(svc)
	freezeSessionAdmission(t, session)

	line := fileCommandLineOfSize(t, 4096)

	// One byte less than the stream would cost if the stream were charged here.
	const stream = 8
	budget := float64(stream*len(line)) - 1
	setSessionRawBudget(session, budget)

	for i := 0; i < stream; i++ {
		writeWireLine(t, peerEnd, line)
	}
	// The router is the reader's own downstream, and the charge — if it happened
	// at all — happens before the dispatch, so waiting on the hits is what makes
	// the assertion below race-free.
	if got := awaitFileRouterHits(cache, stream); got != stream {
		t.Fatalf("the file router was reached %d times, want %d: the fixture stopped exercising the file plane", got, stream)
	}

	if spent := budget - sessionRawBytesRemaining(session); spent != 0 {
		t.Fatalf("a stream of valid file commands spent %v bytes of the shared bucket: the response-plane budget belongs to the planes that have no exemption of their own", spent)
	}

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
		t.Fatalf("the frame after the file_command stream = %q, want ping: it paid for traffic of another plane", frame.Type)
	}
	if got := sessionViolations(session); got != 0 {
		t.Fatalf("the session carries %v violations after a stream of valid file commands and one ping", got)
	}
	select {
	case err := <-session.errCh:
		t.Fatalf("a valid file_command stream ended the session: %v", err)
	default:
	}

	// The positive control: same size, same count, same bucket, not a file
	// command.
	control := sameSizeContactsLine(t, len(line))
	setSessionRawBudget(session, budget)
	for i := 0; i < stream; i++ {
		writeWireLine(t, peerEnd, control)
	}

	writeWireLine(t, peerEnd, `{"type":"ping"}`+"\n")
	waitForCondition(t, 5*time.Second, func() bool {
		return sessionViolations(session) >= 1
	})
	requireNoInboxFrame(t, session, "a ping behind a stream of non-exempt lines that emptied the shared bucket")

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
		t.Fatalf("%d over-budget lines of a scored type did not end the session: the file_command rule leaked into the general case",
			peerSessionViolationBudget+1)
	}
}

// ---------------------------------------------------------------------------
// The exemption is bought by admissibility, never by the claim
// ---------------------------------------------------------------------------

// barrierPingLine is written AFTER the line under test and waited for on the
// session inbox. The reader is one goroutine consuming one pipe in order, so a
// frame that reaches the inbox proves the line before it has been fully
// processed — which is what makes a bucket read afterwards an observation
// rather than a race.
//
// It is an ordinary line and is charged like one, so every assertion below
// expects its bytes on top of the line it is a barrier for.
const barrierPingLine = `{"type":"ping"}` + "\n"

// TestFileCommandClaimAloneBuysNoExemption is the other half, stated end to end
// on the three ways a line can NAME the file plane without being a frame of it.
//
// Each case is a way to turn the rule into a free channel, and each is asserted
// on the BUCKET rather than on the drop: a decoy is refused either way, so only
// the charge separates "refused after paying" from "refused having switched the
// meter off".
func TestFileCommandClaimAloneBuysNoExemption(t *testing.T) {
	t.Parallel()

	t.Run("ambiguous line that claims the type in its first bytes", func(t *testing.T) {
		t.Parallel()

		svc, peerEnd, session := newFileCommandReadFixture(t, domain.CapFileTransferV1)
		installRecordingFileRouter(svc)
		freezeSessionAdmission(t, session)

		// The pre-read claim reads `file_command`; encoding/json binds `ping`.
		// The sender picks which reader gets which answer, so the exemption may
		// not be decided on the first one.
		decoy := `{"type":"file_command","type":"ping"}` + "\n"
		before := sessionRawBytesRemaining(session)
		writeWireLine(t, peerEnd, decoy)
		waitForCondition(t, 5*time.Second, func() bool {
			return sessionViolations(session) >= 1
		})

		if spent := before - sessionRawBytesRemaining(session); spent != float64(len(decoy)) {
			t.Fatalf("a line that only CLAIMED file_command spent %v bytes of the shared bucket, want %d: an unresolvable type must fail closed",
				spent, len(decoy))
		}
	})

	t.Run("valid frame on a session that never negotiated file_transfer_v1", func(t *testing.T) {
		t.Parallel()

		svc, peerEnd, session := newFileCommandReadFixture(t, domain.CapMeshRelayV1)
		cache := installRecordingFileRouter(svc)
		freezeSessionAdmission(t, session)

		line := fileCommandLineOfSize(t, 2048)
		before := sessionRawBytesRemaining(session)
		writeWireLine(t, peerEnd, line)
		writeWireLine(t, peerEnd, barrierPingLine)
		if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
			t.Fatalf("frame after the file_command = %q, want ping", frame.Type)
		}

		if got := cache.count(); got != 0 {
			t.Fatalf("the file router was reached %d times on a session without file_transfer_v1: the fixture stopped exercising the gate", got)
		}
		want := float64(len(line) + len(barrierPingLine))
		if spent := before - sessionRawBytesRemaining(session); spent != want {
			t.Fatalf("a file_command this reader will never act on spent %v bytes of the shared bucket, want %v: an exemption a peer gets by naming a type it never negotiated is a free channel",
				spent, want)
		}
	})

	t.Run("valid frame on a node with no file router", func(t *testing.T) {
		t.Parallel()

		_, peerEnd, session := newFileCommandReadFixture(t, domain.CapFileTransferV1)
		freezeSessionAdmission(t, session)

		line := fileCommandLineOfSize(t, 2048)
		before := sessionRawBytesRemaining(session)
		writeWireLine(t, peerEnd, line)
		writeWireLine(t, peerEnd, barrierPingLine)
		if frame := awaitInboxFrame(t, session); frame.Type != "ping" {
			t.Fatalf("frame after the file_command = %q, want ping", frame.Type)
		}

		want := float64(len(line) + len(barrierPingLine))
		if spent := before - sessionRawBytesRemaining(session); spent != want {
			t.Fatalf("a file_command spent %v bytes of the shared bucket on a node with no consumer for it, want %v: no subsystem, no exemption",
				spent, want)
		}
	})
}

// TestFileCommandExemptionAnswersFromTheAuthoritativeClassification pins the
// three inputs of the predicate one by one, because each of them alone is a way
// to buy a free channel.
//
// The line half is asserted against the SAME classification the dispatch branch
// resolves from: an exemption granted to a line the dispatcher then treats as
// another type would be a line metered by nobody. The entitlement half is
// asserted by removing one condition at a time from an otherwise valid frame.
func TestFileCommandExemptionAnswersFromTheAuthoritativeClassification(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, false)
	installRecordingFileRouter(svc)
	session := &peerSession{
		address:      "10.9.9.9:64646",
		capabilities: []domain.Capability{domain.CapFileTransferV1},
		authOK:       true,
	}
	valid := fileCommandLineOfSize(t, 512)

	for name, tc := range map[string]struct {
		line   string
		exempt bool
	}{
		"a real file command":      {line: valid, exempt: true},
		"the bare type":            {line: `{"type":"file_command"}` + "\n", exempt: true},
		"another type":             {line: `{"type":"ping"}` + "\n", exempt: false},
		"duplicate top-level type": {line: `{"type":"file_command","type":"ping"}` + "\n", exempt: false},
		"the type named first but nested": {
			line:   `{"a":{"type":"file_command"},"type":"ping"}` + "\n",
			exempt: false,
		},
		"a case variant encoding/json still binds": {line: `{"TYPE":"file_command"}` + "\n", exempt: false},
		"the type in a value":                      {line: `{"note":"file_command"}` + "\n", exempt: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := svc.sessionFileCommandIsAdmissible(session, tc.line); got != tc.exempt {
				t.Fatalf("sessionFileCommandIsAdmissible(%.48q) = %v, want %v", tc.line, got, tc.exempt)
			}
		})
	}

	t.Run("a session that never negotiated the capability", func(t *testing.T) {
		t.Parallel()
		uncapable := &peerSession{address: session.address, authOK: true}
		if svc.sessionFileCommandIsAdmissible(uncapable, valid) {
			t.Fatal("a file_command skipped the response-plane budget on a session without file_transfer_v1: the frame is dropped unread by the gate below, so the exemption spares a line with no possible effect")
		}
	})

	t.Run("a session whose handshake has not completed", func(t *testing.T) {
		t.Parallel()
		unauthenticated := &peerSession{address: session.address, capabilities: session.capabilities}
		if svc.sessionFileCommandIsAdmissible(unauthenticated, valid) {
			t.Fatal("a file_command skipped the response-plane budget on a session that has not finished authenticating")
		}
	})

	t.Run("a node with no file router", func(t *testing.T) {
		t.Parallel()
		noConsumer := newDatagramLayerService(t, false)
		if noConsumer.sessionFileCommandIsAdmissible(session, valid) {
			t.Fatal("a file_command skipped the response-plane budget on a node with no router to hand it to")
		}
	})
}
