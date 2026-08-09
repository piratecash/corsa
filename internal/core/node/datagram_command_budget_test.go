package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_command_budget_test.go pins the swap the `datagram` exemption is:
// the plane leaves the per-connection command limiter because it charges its
// own §5 budget instead, so a line that reaches NEITHER must not be exempt.
//
// That is exactly what an unauthenticated connection produces. The inbound
// dispatcher answers `auth_required` above the ingress, so nothing on such a
// connection is ever charged the datagram budget — its key is the identity the
// neighbour PROVED, and before `auth_ok` there is none. The exemption used to
// be taken from the layer's existence alone, so a stranger could repeat
// datagram lines at line rate and make this node build and write a synchronous
// error frame for each of them, for free.

// unregisteredConnID is the ConnID of no connection at all — what a test that
// asks the rate decision about a line rather than about a peer hands it. The
// counter never issues zero, so the lookups behind the decision miss and the
// datagram half answers "no billable key", which is the fail-closed side.
const unregisteredConnID = domain.ConnID(0)

// registerDatagramCommandConn registers ONE accepted connection and returns its
// ConnID. `authenticated` decides whether it carries the verified hello the
// inbound datagram budget key is derived from — the same state
// handleAuthSession leaves behind after connauth.VerifyAuthSession.
func registerDatagramCommandConn(t *testing.T, svc *Service, id domain.ConnID, authenticated bool) domain.ConnID {
	t.Helper()

	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	core := netcore.New(id, serverPipe, netcore.Inbound, netcore.Options{})
	t.Cleanup(core.Close)
	if authenticated {
		core.SetAuth(&connauth.State{
			Verified: true,
			Hello:    protocol.Frame{Address: datagramTestDstHex},
		})
	}

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientPipe, &connEntry{core: core, tracked: true})
	svc.peerMu.Unlock()
	return id
}

// runCommandLineFlood replays one line through the read loop's whole rate
// decision and reports how many of the attempts it refused. The read loop closes
// the connection on the first refusal; the count is what makes "refused at all"
// distinguishable from "refused throughout".
func runCommandLineFlood(svc *Service, id domain.ConnID, connKey, line string, attempts int) int {
	refused := 0
	for i := 0; i < attempts; i++ {
		if !svc.admitInboundCommandLine(id, connKey, line) {
			refused++
		}
	}
	return refused
}

// TestPreAuthDatagramStreamMeetsTheCommandLimiter is the finding.
//
// The mutation it kills: taking the exemption from the layer's existence alone,
// without asking whether this connection has a key the replacement budget can
// be charged on.
func TestPreAuthDatagramStreamMeetsTheCommandLimiter(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)
	line := mustDatagramLine(t, newNodeDatagram(t, nil))

	// Past the burst with headroom: a limiter that covers this stream at all
	// must refuse here, and a limiter that does not cover it refuses nothing.
	const attempts = cmdBurstPerConn + 50

	stranger := registerDatagramCommandConn(t, svc, domain.ConnID(8801), false)
	refused := runCommandLineFlood(svc, stranger, "203.0.113.11:1001", line, attempts)
	if refused == 0 {
		t.Fatalf("%d pre-auth datagram lines all passed the rate decision: the line is exempt from "+
			"the command limiter and the layer's budget never sees it either, so the refusal "+
			"this node answers it with costs the sender nothing", attempts)
	}

	// The other half, and the reason the fix is not "drop the exemption":
	// an authenticated neighbour is billed by §5 and must not be billed twice.
	// Its bulk chunks do not fit a control-plane rate, and the limiter's answer
	// is a tear-down with ban points rather than a drop.
	neighbour := registerDatagramCommandConn(t, svc, domain.ConnID(8802), true)
	if refused := runCommandLineFlood(svc, neighbour, "203.0.113.12:1002", line, attempts); refused != 0 {
		t.Fatalf("%d of %d datagram lines from an AUTHENTICATED neighbour were charged the command "+
			"limiter: that plane already pays the §5 budget, and paying both throttles it to a "+
			"control-plane rate", refused, attempts)
	}
}

// TestDatagramExemptionFollowsTheBillableKey states the same rule at the level
// of the decision itself, on ONE connection, so the boundary is visible rather
// than inferred from a flood: the very same line is exempt after `auth_ok` and
// not before.
func TestDatagramExemptionFollowsTheBillableKey(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)
	line := mustDatagramLine(t, newNodeDatagram(t, nil))
	id := registerDatagramCommandConn(t, svc, domain.ConnID(8803), false)

	if svc.frameLineExemptFromCommandLimit(id, line) {
		t.Fatal("a datagram on an unauthenticated connection was exempted although its §5 budget " +
			"has no key to charge")
	}

	core := svc.netCoreForID(id)
	if core == nil {
		t.Fatal("the fixture connection is not registered")
	}
	core.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: datagramTestDstHex}})

	if !svc.frameLineExemptFromCommandLimit(id, line) {
		t.Fatal("a datagram from an authenticated neighbour lost its exemption although the layer " +
			"charges its own budget for it")
	}
}
