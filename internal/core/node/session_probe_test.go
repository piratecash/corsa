package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// oldReleaseCapabilities is what a v2.4.70 full node with mesh routing v3
// and both datagram roles enabled puts into welcome (localCapabilities of
// that release).
var oldReleaseCapabilities = []string{
	string(domain.CapMeshRelayV1),
	string(domain.CapMeshRoutingV1),
	string(domain.CapMeshRoutingV2),
	string(domain.CapFileTransferV1),
	string(domain.CapMeshRouteProbeV1),
	string(domain.CapMeshRouteQueryV1),
	string(domain.CapMeshRouteSyncV1),
	string(domain.CapMeshRoutingV3),
	string(domain.CapMeshPoisonReverseV1),
	string(domain.CapMeshPoisonReverseV2),
	string(domain.CapMeshDatagramV1),
	string(domain.CapMeshDatagramTransitV1),
}

// oldReleaseWelcome is the welcome a v28–v30 release (v2.4.70 and earlier)
// answers a hello without identity fields with: its full self-description
// (welcomeFrame of that release) and no challenge, because the identity-less
// branch of its hello handler calls sendWelcomeFrame with an empty challenge.
func oldReleaseWelcome(t *testing.T, id *identity.Identity, version int) protocol.Frame {
	t.Helper()
	dtypes := []string{
		string(domain.DTypeDMControl),
		string(domain.DTypeGetIdentity),
		string(domain.DTypePostIdentity),
		string(domain.DTypePushIdentity),
	}
	return protocol.Frame{
		Type:                   "welcome",
		Version:                version,
		MinimumProtocolVersion: 26,
		Node:                   nodeName,
		Network:                networkName,
		Listener:               "1",
		AdvertisePort:          64646,
		NodeType:               "full",
		ClientVersion:          "2.4.70",
		ClientBuild:            70,
		Address:                id.Address,
		PubKey:                 identity.PublicKeyBase64(id.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(id),
		ObservedAddress:        "127.0.0.1",
		Services:               []string{"identity", "contacts", "messages", "gazeta", "relay"},
		Capabilities:           append([]string(nil), oldReleaseCapabilities...),
		DTypes:                 &dtypes,
	}
}

// welcomeLineWith marshals frame and lets mutate edit its JSON object, so a
// case can drop a key or set a value the Frame struct would omit (version 0).
func welcomeLineWith(t *testing.T, frame protocol.Frame, mutate func(map[string]any)) string {
	t.Helper()
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal welcome: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("unmarshal welcome: %v", err)
	}
	mutate(object)
	edited, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal edited welcome: %v", err)
	}
	return string(edited) + "\n"
}

func generateProbeIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id
}

func TestLegacyProbeLineCarriesOnlyTypeVersionClientNetwork(t *testing.T) {
	line, err := legacyProbeLine()
	if err != nil {
		t.Fatalf("legacyProbeLine: %v", err)
	}
	if !bytes.HasSuffix(line, []byte("\n")) {
		t.Fatalf("probe is not a newline-terminated frame line: %q", line)
	}
	var object map[string]any
	if err := json.Unmarshal(line, &object); err != nil {
		t.Fatalf("probe is not a JSON object: %v", err)
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{"client", "network", "type", "version"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("probe keys = %v, want exactly %v", keys, want)
	}
	frame, err := protocol.ParseFrameLine(strings.TrimSpace(string(line)))
	if err != nil {
		t.Fatalf("parse probe: %v", err)
	}
	if frame.Type != "hello" || frame.Version != config.ProtocolVersion || frame.Client != "node" || frame.Network != networkName {
		t.Fatalf("probe = %+v, want hello v%d from node on %s", frame, config.ProtocolVersion, networkName)
	}
}

func TestClassifyLegacyProbeLine(t *testing.T) {
	peer := generateProbeIdentity(t)
	stranger := generateProbeIdentity(t)
	legacy := oldReleaseWelcome(t, peer, 30)
	line := func(mutate func(map[string]any)) string { return welcomeLineWith(t, legacy, mutate) }
	keep := func(map[string]any) {}

	cases := []struct {
		name    string
		line    string
		outcome legacyProbeOutcome
		err     error
	}{
		{name: "v30 welcome from an old release", line: line(keep), outcome: legacyProbeOutcomeLegacyAllowed},
		{name: "welcome at the floor", line: line(func(o map[string]any) { o["version"] = config.MinimumProtocolVersion }), outcome: legacyProbeOutcomeLegacyAllowed},
		{name: "welcome just below v2", line: line(func(o map[string]any) { o["version"] = config.ProtocolVersionSecureSession - 1 }), outcome: legacyProbeOutcomeLegacyAllowed},
		{name: "welcome without version", line: line(func(o map[string]any) { delete(o, "version") }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeVersionMissing},
		{name: "welcome with version 0", line: line(func(o map[string]any) { o["version"] = 0 }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeVersionMissing},
		{name: "welcome with negative version", line: line(func(o map[string]any) { o["version"] = -1 }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeVersionMissing},
		{name: "welcome below the floor", line: line(func(o map[string]any) { o["version"] = config.MinimumProtocolVersion - 1 }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeVersionTooOld},
		{name: "welcome at the v2 version", line: line(func(o map[string]any) { o["version"] = config.ProtocolVersionSecureSession }), outcome: legacyProbeOutcomeV2Hint},
		{name: "welcome above the v2 version", line: line(func(o map[string]any) { o["version"] = config.ProtocolVersionSecureSession + 9 }), outcome: legacyProbeOutcomeV2Hint},
		{name: "welcome with a challenge", line: line(func(o map[string]any) { o["challenge"] = "c" }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeChallengePresent},
		{name: "v2 welcome with a challenge", line: line(func(o map[string]any) {
			o["version"] = config.ProtocolVersionSecureSession
			o["challenge"] = "c"
		}), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeChallengePresent},
		{name: "welcome from another network", line: line(func(o map[string]any) { o["network"] = "elsewhere" }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeNetworkMismatch},
		{name: "welcome without network", line: line(func(o map[string]any) { delete(o, "network") }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeNetworkMismatch},
		{name: "welcome without address", line: line(func(o map[string]any) { delete(o, "address") }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeIdentityMissing},
		{name: "welcome without pubkey", line: line(func(o map[string]any) { delete(o, "pubkey") }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeIdentityMissing},
		{name: "welcome without boxkey", line: line(func(o map[string]any) { delete(o, "boxkey") }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeIdentityMissing},
		{name: "welcome without boxsig", line: line(func(o map[string]any) { delete(o, "boxsig") }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeIdentityMissing},
		{name: "address not derived from pubkey", line: line(func(o map[string]any) { o["address"] = stranger.Address }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeAddressMismatch},
		{name: "pubkey that does not parse", line: line(func(o map[string]any) { o["pubkey"] = "not-a-key" }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeAddressMismatch},
		{name: "boxsig by another identity", line: line(func(o map[string]any) { o["boxsig"] = identity.SignBoxKeyBinding(stranger) }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeBoxSigInvalid},
		{name: "boxkey other than the signed one", line: line(func(o map[string]any) { o["boxkey"] = identity.BoxPublicKeyBase64(stranger.BoxPublicKey) }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeBoxSigInvalid},
		{name: "invalid-json error frame", line: `{"type":"error","code":"invalid-json","error":"bad"}` + "\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeErrorFrame},
		{name: "incompatible-protocol error frame", line: `{"type":"error","code":"incompatible-protocol","version":30,"minimum_protocol_version":26}` + "\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeErrorFrame},
		{name: "frame other than welcome", line: `{"type":"connection_notice","version":30}` + "\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeUnexpectedFrame},
		{name: "frame without type", line: line(func(o map[string]any) { delete(o, "type") }), outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeUnexpectedFrame},
		{name: "two frames on one line", line: strings.TrimSuffix(line(keep), "\n") + strings.TrimSuffix(line(keep), "\n") + "\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeMalformed},
		{name: "truncated JSON", line: `{"type":"welcome","version":30` + "\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeMalformed},
		{name: "garbage", line: "SSH-2.0-OpenSSH_9.6\r\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeMalformed},
		{name: "TLS record bytes", line: "\x16\x03\x03\x00\x02\x02\x50\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeMalformed},
		{name: "empty line", line: "\n", outcome: legacyProbeOutcomeNotLegacy, err: errLegacyProbeMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome, reply, err := classifyLegacyProbeLine(tc.line)
			if outcome != tc.outcome {
				t.Fatalf("outcome = %v, want %v (err %v)", outcome, tc.outcome, err)
			}
			if tc.err == nil && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.err)
			}
			wantReply := tc.err == nil || !errors.Is(tc.err, errLegacyProbeMalformed)
			if (reply != nil) != wantReply {
				t.Fatalf("reply present = %v, want %v", reply != nil, wantReply)
			}
		})
	}
}

// fakeOldNode plays the listener side of net.Pipe: it reads the probe line,
// hands it to the test, and then runs answer on the connection.
type fakeOldNode struct {
	probe chan string
	done  chan struct{}
}

func startFakeOldNode(conn net.Conn, answer func(net.Conn)) *fakeOldNode {
	node := &fakeOldNode{probe: make(chan string, 1), done: make(chan struct{})}
	go func() {
		defer close(node.done)
		line, err := bufio.NewReader(conn).ReadString('\n')
		node.probe <- line
		if err != nil {
			return
		}
		answer(conn)
	}()
	return node
}

func writeOrIgnore(conn net.Conn, payload string) {
	// The probe side may refuse the line and stop reading; the write then
	// fails when the test closes the pipe, which is the end of this fake.
	_, _ = conn.Write([]byte(payload))
}

func TestProbeLegacyAgainstOldReleaseAllowsLegacyAndKeepsTheRemainder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peer := generateProbeIdentity(t)
		welcome, err := protocol.MarshalFrameLine(oldReleaseWelcome(t, peer, 30))
		if err != nil {
			t.Fatalf("marshal welcome: %v", err)
		}
		const trailing = `{"type":"ping"}` + "\n"
		const late = `{"type":"pong"}` + "\n"

		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		defer func() { _ = remote.Close() }()
		opts := defaultLegacyProbeOptions()
		node := startFakeOldNode(remote, func(conn net.Conn) {
			writeOrIgnore(conn, welcome+trailing)
			time.Sleep(2 * opts.timeout)
			writeOrIgnore(conn, late)
		})

		answer, err := probeLegacy(context.Background(), local, opts)
		if err != nil {
			t.Fatalf("probeLegacy: %v", err)
		}
		if answer.outcome != legacyProbeOutcomeLegacyAllowed {
			t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeLegacyAllowed)
		}
		if answer.reply == nil || answer.reply.Address != peer.Address {
			t.Fatalf("reply = %+v, want the old node's welcome", answer.reply)
		}
		wantProbe, err := legacyProbeLine()
		if err != nil {
			t.Fatalf("legacyProbeLine: %v", err)
		}
		if got := <-node.probe; got != string(wantProbe) {
			t.Fatalf("old node read %q, want the probe %q", got, wantProbe)
		}
		next, err := answer.reader.ReadString('\n')
		if err != nil || next != trailing {
			t.Fatalf("remainder = %q (%v), want %q", next, err, trailing)
		}
		// A line arriving after the probe's deadline would have passed must
		// still be readable: the probe hands the connection back without one.
		next, err = answer.reader.ReadString('\n')
		if err != nil || next != late {
			t.Fatalf("late line = %q (%v), want %q", next, err, late)
		}
		<-node.done
	})
}

func TestProbeLegacyReportsAV2Hint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peer := generateProbeIdentity(t)
		welcome, err := protocol.MarshalFrameLine(oldReleaseWelcome(t, peer, config.ProtocolVersionSecureSession))
		if err != nil {
			t.Fatalf("marshal welcome: %v", err)
		}
		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		defer func() { _ = remote.Close() }()
		node := startFakeOldNode(remote, func(conn net.Conn) { writeOrIgnore(conn, welcome) })

		answer, err := probeLegacy(context.Background(), local, defaultLegacyProbeOptions())
		if err != nil {
			t.Fatalf("probeLegacy: %v", err)
		}
		if answer.outcome != legacyProbeOutcomeV2Hint {
			t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeV2Hint)
		}
		<-node.done
	})
}

func TestProbeLegacyRefusesWhatIsNotAnOldWelcome(t *testing.T) {
	tooLong := strings.Repeat("a", protocol.MaxFrameLine+1)
	cases := []struct {
		name   string
		answer func(net.Conn)
		err    error
	}{
		{name: "line longer than the unauthenticated limit", answer: func(c net.Conn) { writeOrIgnore(c, tooLong) }, err: errLegacyProbeLineTooLong},
		{name: "connection cut before the newline", answer: func(c net.Conn) {
			writeOrIgnore(c, `{"type":"welcome","version":30`)
			_ = c.Close()
		}, err: errLegacyProbeNoAnswer},
		{name: "connection closed without an answer", answer: func(c net.Conn) { _ = c.Close() }, err: errLegacyProbeNoAnswer},
		{name: "error frame", answer: func(c net.Conn) {
			writeOrIgnore(c, `{"type":"error","code":"invalid-json"}`+"\n")
		}, err: errLegacyProbeErrorFrame},
		{name: "garbage line", answer: func(c net.Conn) { writeOrIgnore(c, "HTTP/1.1 400 Bad Request\r\n") }, err: errLegacyProbeMalformed},
		{name: "empty line", answer: func(c net.Conn) { writeOrIgnore(c, "\n") }, err: errLegacyProbeMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				local, remote := net.Pipe()
				node := startFakeOldNode(remote, tc.answer)

				answer, err := probeLegacy(context.Background(), local, defaultLegacyProbeOptions())
				_ = local.Close()
				_ = remote.Close()
				<-node.done
				if !errors.Is(err, tc.err) {
					t.Fatalf("err = %v, want errors.Is %v", err, tc.err)
				}
				if answer.outcome != legacyProbeOutcomeNotLegacy {
					t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeNotLegacy)
				}
			})
		})
	}
}

func TestProbeLegacyTimesOutOnTheConfiguredDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		defer func() { _ = remote.Close() }()
		release := make(chan struct{})
		node := startFakeOldNode(remote, func(net.Conn) { <-release })

		opts := defaultLegacyProbeOptions()
		opts.timeout = 7 * time.Second
		start := time.Now()
		answer, err := probeLegacy(context.Background(), local, opts)
		elapsed := time.Since(start)
		close(release)
		<-node.done

		if !errors.Is(err, errLegacyProbeTimeout) {
			t.Fatalf("err = %v, want errors.Is %v", err, errLegacyProbeTimeout)
		}
		if answer.outcome != legacyProbeOutcomeNotLegacy {
			t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeNotLegacy)
		}
		if elapsed != opts.timeout {
			t.Fatalf("probe gave up after %v, want exactly the configured %v", elapsed, opts.timeout)
		}
	})
}

func TestProbeLegacyDeadlineFollowsTheInjectedClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		defer func() { _ = remote.Close() }()
		release := make(chan struct{})
		node := startFakeOldNode(remote, func(net.Conn) { <-release })

		// A clock running 4 s behind the bubble's puts the absolute deadline
		// 4 s earlier: the probe reads time only through opts.systemNow.
		const skew = 4 * time.Second
		opts := defaultLegacyProbeOptions()
		opts.timeout = 10 * time.Second
		opts.systemNow = func() time.Time { return time.Now().Add(-skew) }
		start := time.Now()
		_, err := probeLegacy(context.Background(), local, opts)
		elapsed := time.Since(start)
		close(release)
		<-node.done

		if !errors.Is(err, errLegacyProbeTimeout) {
			t.Fatalf("err = %v, want errors.Is %v", err, errLegacyProbeTimeout)
		}
		if elapsed != opts.timeout-skew {
			t.Fatalf("probe gave up after %v, want %v", elapsed, opts.timeout-skew)
		}
	})
}

func TestProbeLegacyStopsWhenTheContextIsCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		defer func() { _ = remote.Close() }()
		release := make(chan struct{})
		node := startFakeOldNode(remote, func(net.Conn) { <-release })

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-node.probe
			cancel()
		}()
		start := time.Now()
		answer, err := probeLegacy(ctx, local, defaultLegacyProbeOptions())
		elapsed := time.Since(start)
		close(release)
		<-node.done

		if !errors.Is(err, errLegacyProbeAborted) || !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want errors.Is %v and context.Canceled", err, errLegacyProbeAborted)
		}
		if answer.outcome != legacyProbeOutcomeNotLegacy {
			t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeNotLegacy)
		}
		if elapsed != 0 {
			t.Fatalf("probe returned after %v, want at once on cancel", elapsed)
		}
	})
}

// writeRefusingConn takes deadlines but refuses every write, so a test can
// fail the write alone: a closed net.Pipe already refuses SetDeadline.
type writeRefusingConn struct {
	net.Conn
}

var errTestWriteRefused = errors.New("test: write refused")

func (writeRefusingConn) Write([]byte) (int, error) { return 0, errTestWriteRefused }

func TestProbeLegacyReportsAFailedWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		defer func() { _ = remote.Close() }()

		answer, err := probeLegacy(context.Background(), writeRefusingConn{Conn: local}, defaultLegacyProbeOptions())
		if !errors.Is(err, errLegacyProbeWrite) || !errors.Is(err, errTestWriteRefused) {
			t.Fatalf("err = %v, want errors.Is %v", err, errLegacyProbeWrite)
		}
		if answer.outcome != legacyProbeOutcomeNotLegacy {
			t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeNotLegacy)
		}
	})
}

func TestProbeLegacyRefusesIncompleteOptions(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*legacyProbeOptions)
	}{
		{name: "zero timeout", mutate: func(o *legacyProbeOptions) { o.timeout = 0 }},
		{name: "negative timeout", mutate: func(o *legacyProbeOptions) { o.timeout = -time.Second }},
		{name: "no clock", mutate: func(o *legacyProbeOptions) { o.systemNow = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			local, remote := net.Pipe()
			defer func() { _ = local.Close() }()
			defer func() { _ = remote.Close() }()
			opts := defaultLegacyProbeOptions()
			tc.mutate(&opts)

			answer, err := probeLegacy(context.Background(), local, opts)
			if !errors.Is(err, errLegacyProbeOptionsInvalid) {
				t.Fatalf("err = %v, want errors.Is %v", err, errLegacyProbeOptionsInvalid)
			}
			if answer.outcome != legacyProbeOutcomeNotLegacy {
				t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeNotLegacy)
			}
		})
	}
}

func TestLegacyProbeOutcomeNames(t *testing.T) {
	cases := map[legacyProbeOutcome]string{
		legacyProbeOutcomeNotLegacy:     "not_legacy",
		legacyProbeOutcomeLegacyAllowed: "legacy_allowed",
		legacyProbeOutcomeV2Hint:        "v2_hint",
		legacyProbeOutcome(0):           "unknown",
	}
	for outcome, want := range cases {
		if got := outcome.String(); got != want {
			t.Fatalf("legacyProbeOutcome(%d).String() = %q, want %q", int(outcome), got, want)
		}
	}
}

// legacyProbeAnswerMarker is a byte string no error text could contain by
// accident; every answer below plants it (or a variant) where the remote side
// chooses the bytes, so an error that echoes the answer shows it.
const legacyProbeAnswerMarker = "QZXV7MARKER"

func TestLegacyProbeErrorsCarryNoAnswerBytes(t *testing.T) {
	peer := generateProbeIdentity(t)
	legacy := oldReleaseWelcome(t, peer, 30)
	line := func(mutate func(map[string]any)) string { return welcomeLineWith(t, legacy, mutate) }
	marked := func(field string) string { return legacyProbeAnswerMarker + "_" + field }

	cases := []struct {
		name string
		line string
		// leaks are fragments of THIS answer that must not reach the error,
		// on top of the marker itself.
		leaks []string
	}{
		{name: "garbage", line: legacyProbeAnswerMarker + " garbage\n", leaks: []string{"'Q'"}},
		{name: "number where version goes", line: `{"type":"welcome","version":123456789.5}` + "\n", leaks: []string{"123456789.5"}},
		{name: "string where version goes", line: `{"type":"welcome","version":"` + marked("version") + `"}` + "\n"},
		{name: "object where services go", line: `{"type":"welcome","services":{"` + marked("services") + `":1}}` + "\n"},
		{name: "truncated after a marker", line: `{"type":"welcome","node":"` + marked("node") + "\n"},
		{name: "unknown type", line: line(func(o map[string]any) { o["type"] = marked("type") })},
		{name: "error frame", line: `{"type":"error","code":"` + marked("code") + `","error":"` + marked("error") + `"}` + "\n"},
		{name: "network", line: line(func(o map[string]any) { o["network"] = marked("network") })},
		{name: "challenge", line: line(func(o map[string]any) { o["challenge"] = marked("challenge") })},
		{name: "version below the floor", line: line(func(o map[string]any) {
			o["version"] = 27
			o["client_version"] = marked("client_version")
		}), leaks: []string{"27"}},
		{name: "address", line: line(func(o map[string]any) { o["address"] = marked("address") })},
		{name: "pubkey", line: line(func(o map[string]any) { o["pubkey"] = marked("pubkey") })},
		{name: "boxkey", line: line(func(o map[string]any) { o["boxkey"] = marked("boxkey") })},
		{name: "boxsig", line: line(func(o map[string]any) { o["boxsig"] = marked("boxsig") })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := classifyLegacyProbeLine(tc.line)
			assertLegacyProbeErrorCarriesNoAnswer(t, err, tc.leaks)
		})
	}

	readerCases := []struct {
		name   string
		answer string
		close  bool
	}{
		{name: "garbage line", answer: legacyProbeAnswerMarker + " garbage\n"},
		{name: "cut before the newline", answer: `{"type":"welcome","node":"` + marked("node"), close: true},
		{name: "longer than the limit", answer: strings.Repeat(legacyProbeAnswerMarker, protocol.MaxFrameLine/len(legacyProbeAnswerMarker)+1)},
	}
	for _, tc := range readerCases {
		t.Run("probe "+tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				local, remote := net.Pipe()
				node := startFakeOldNode(remote, func(c net.Conn) {
					writeOrIgnore(c, tc.answer)
					if tc.close {
						_ = c.Close()
					}
				})
				_, err := probeLegacy(context.Background(), local, defaultLegacyProbeOptions())
				_ = local.Close()
				_ = remote.Close()
				<-node.done
				assertLegacyProbeErrorCarriesNoAnswer(t, err, []string{"'Q'"})
			})
		})
	}
}

func assertLegacyProbeErrorCarriesNoAnswer(t *testing.T, err error, leaks []string) {
	t.Helper()
	if err == nil {
		t.Fatal("err = nil, want a refusal")
	}
	for _, fragment := range append([]string{legacyProbeAnswerMarker}, leaks...) {
		if strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q carries answer bytes %q", err, fragment)
		}
	}
}

func TestProbeLegacyAcceptsTheLargestRealisticWelcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peer := generateProbeIdentity(t)
		welcome := oldReleaseWelcome(t, peer, 30)
		// The most a release can declare: every capability this tree knows,
		// eight dtypes of the longest lawful name, every service.
		welcome.Capabilities = append(append([]string(nil), oldReleaseCapabilities...), string(domain.CapMeshAttestedLinksV1))
		dtypes := make([]string, 0, 8)
		for i := range 8 {
			dtypes = append(dtypes, strings.Repeat(string(rune('a'+i)), domain.MaxDTypeLen))
		}
		welcome.DTypes = &dtypes
		line, err := protocol.MarshalFrameLine(welcome)
		if err != nil {
			t.Fatalf("marshal welcome: %v", err)
		}
		if len(line) > protocol.MaxFrameLine {
			t.Fatalf("largest welcome is %d bytes, over the %d-byte probe limit", len(line), protocol.MaxFrameLine)
		}

		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		defer func() { _ = remote.Close() }()
		node := startFakeOldNode(remote, func(c net.Conn) { writeOrIgnore(c, line) })
		answer, err := probeLegacy(context.Background(), local, defaultLegacyProbeOptions())
		<-node.done
		if err != nil || answer.outcome != legacyProbeOutcomeLegacyAllowed {
			t.Fatalf("outcome = %v (%v), want %v", answer.outcome, err, legacyProbeOutcomeLegacyAllowed)
		}
	})
}

func TestProbeLegacyRefusesALineOverTheUnauthenticatedLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, remote := net.Pipe()
		// Exactly one byte over, and then silence: a reader that allowed more
		// would wait for the rest and time out instead of refusing.
		node := startFakeOldNode(remote, func(c net.Conn) { writeOrIgnore(c, strings.Repeat("a", protocol.MaxFrameLine+1)) })
		_, err := probeLegacy(context.Background(), local, defaultLegacyProbeOptions())
		_ = local.Close()
		_ = remote.Close()
		<-node.done
		if !errors.Is(err, errLegacyProbeLineTooLong) {
			t.Fatalf("err = %v, want errors.Is %v", err, errLegacyProbeLineTooLong)
		}
	})
}

func TestProbeLegacyReadsATLSAlertAsAV2Hint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, remote := net.Pipe()
		defer func() { _ = local.Close() }()
		// The listener's refusal record (sessionv2.RefusalAlert), and then it
		// closes.
		node := startFakeOldNode(remote, func(c net.Conn) {
			writeOrIgnore(c, string(sessionv2.RefusalAlert()))
			_ = c.Close()
		})
		answer, err := probeLegacy(context.Background(), local, defaultLegacyProbeOptions())
		<-node.done
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if answer.outcome != legacyProbeOutcomeV2Hint {
			t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeV2Hint)
		}
		if answer.reply != nil {
			t.Fatalf("reply = %+v, want none for an alert", answer.reply)
		}
	})
}

func TestProbeLegacyReportsAFailedDeadlineApartFromAFailedWrite(t *testing.T) {
	local, remote := net.Pipe()
	_ = local.Close()
	defer func() { _ = remote.Close() }()

	answer, err := probeLegacy(context.Background(), local, defaultLegacyProbeOptions())
	if !errors.Is(err, errLegacyProbeDeadline) {
		t.Fatalf("err = %v, want errors.Is %v", err, errLegacyProbeDeadline)
	}
	if errors.Is(err, errLegacyProbeWrite) {
		t.Fatalf("err = %v, a deadline failure must not read as a write failure", err)
	}
	if answer.outcome != legacyProbeOutcomeNotLegacy {
		t.Fatalf("outcome = %v, want %v", answer.outcome, legacyProbeOutcomeNotLegacy)
	}
}
