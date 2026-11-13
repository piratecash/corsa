//go:build s6_direct_dmv2_qa

package sdk

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func TestSDKDirectDMV2WakeOnProvedSession(t *testing.T) {
	t.Run("before held poll", func(t *testing.T) { runSDKDirectDMV2Wake(t, false, false, false) })
	t.Run("during stale arm", func(t *testing.T) { runSDKDirectDMV2Wake(t, true, false, false) })
}

func TestSDKDirectDMV2WakeOnAcceptedSession(t *testing.T) {
	t.Run("before held poll", func(t *testing.T) { runSDKDirectDMV2Wake(t, false, true, false) })
	t.Run("during stale arm", func(t *testing.T) { runSDKDirectDMV2Wake(t, true, true, false) })
}

func TestSDKDirectDMV2WakeOnBootstrapTimer(t *testing.T) {
	runSDKDirectDMV2Wake(t, false, true, true)
}

func runSDKDirectDMV2Wake(t *testing.T, staleArm, inbound, realTimer bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	cfg := testRuntimeConfig(t)
	cfg.App.Network = "gazeta-devnet"
	cfg.Node.Type = NodeTypeFull
	cfg.Node.AllowPrivatePeers = true
	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := runtime.client.EnableDirectDMV2SendForTest(time.Hour); err != nil {
		t.Fatal(err)
	}
	if inbound {
		runtime.nodeService.EnableDirectDMV2InboundWakeTest()
	}
	var testClock atomic.Int64
	testClock.Store(time.Now().UnixNano())
	if !realTimer {
		runtime.nodeService.SetDirectDMV2RetryClockForTest(func() time.Time {
			return time.Unix(0, testClock.Load())
		})
	}
	runTick := func(at time.Time) {
		testClock.Store(at.UnixNano())
		runtime.nodeService.RunDirectDMV2RetryTickForTest(at)
	}
	author, err := identity.Load(cfg.Node.IdentityPath)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if reply := runtime.nodeService.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts", Contacts: []protocol.ContactFrame{{
			Address: recipient.Address, PubKey: identity.PublicKeyBase64(recipient.PublicKey),
			BoxKey: identity.BoxPublicKeyBase64(recipient.BoxPublicKey), BoxSig: identity.SignBoxKeyBinding(recipient),
		}},
	}); reply.Type == "error" {
		t.Fatalf("import recipient: %s", reply.Code)
	}
	msg, err := runtime.SendDirectMessage(ctx, recipient.Address, "scheduler TLSv2 fixture")
	if err != nil || msg == nil || msg.ReceiptStatus != protocol.MessageStatusQueued {
		t.Fatalf("queued SDK send: %v", err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: msg.ID, Sender: author.Address, Recipient: recipient.Address}
	store := chatlog.NewStore(runtime.database.Executor(), domain.PeerIdentityFromWire(author.Address))
	readRow := func() chatlog.Entry {
		t.Helper()
		row, found, err := store.EntryByRef(ctx, ref)
		if err != nil || !found {
			t.Fatalf("outbox row: found=%v err=%v", found, err)
		}
		return row
	}
	original := readRow()
	local, err := sessionv2.NewLocal(author, domain.NetworkID(cfg.App.Network))
	if err != nil {
		t.Fatal(err)
	}
	remote, err := sessionv2.NewLocal(recipient, domain.NetworkID(cfg.App.Network))
	if err != nil {
		t.Fatal(err)
	}
	certificates, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		t.Fatal(err)
	}

	type link struct {
		raw            *retryWriteWitness
		reader         *bufio.Reader
		initialMessage *protocol.Frame
		close          func()
	}
	connectOutbound := func(capable bool) link {
		t.Helper()
		left, right := net.Pipe()
		serverAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 45001}
		clientAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 45002}
		raw := &retryWriteWitness{Conn: left, local: clientAddr, remote: serverAddr}
		t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
		type acceptResult struct {
			proof sessionv2.ProvenSession
			err   error
		}
		accepted := make(chan acceptResult, 1)
		go func() {
			welcome := protocol.Frame{
				Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion,
				NodeType: string(domain.NodeTypeFull),
			}
			if capable {
				welcome.Capabilities = []string{string(domain.CapDirectDMV2)}
			}
			proof, err := sessionv2.Accept(ctx, right, remote, welcome, certificates, sessionv2.DefaultTimeouts)
			accepted <- acceptResult{proof, err}
		}()
		proven, err := sessionv2.Dial(ctx, raw, local, protocol.Frame{
			Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion,
			NodeType: string(domain.NodeTypeFull), Capabilities: []string{string(domain.CapDirectDMV2)},
		}, sessionv2.ExpectPeer(domain.PeerIdentityFromWire(recipient.Address)), sessionv2.DefaultTimeouts)
		if err != nil {
			t.Fatal(err)
		}
		result := <-accepted
		if result.err != nil {
			t.Fatal(result.err)
		}
		peer, err := result.proof.Conn()
		if err != nil {
			t.Fatal(err)
		}
		if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		session, err := runtime.nodeService.AttachDirectDMV2OutboxTestSession(proven)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(session.CloseTransportForRetryTest)
		if !session.PublishForRetryTest() {
			t.Fatal("actual session registration refused")
		}
		runtime.nodeService.WaitBackground()
		return link{raw: raw, reader: bufio.NewReader(peer), close: session.CloseTransportForRetryTest}
	}
	connectInbound := func(capable bool) link {
		t.Helper()
		left, right := net.Pipe()
		serverAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 45003}
		clientAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 45004}
		raw := &retryWriteWitness{Conn: left, local: serverAddr, remote: clientAddr}
		client := &retryWriteWitness{Conn: right, local: clientAddr, remote: serverAddr}
		done := make(chan struct{})
		go func() {
			defer close(done)
			runtime.nodeService.HandleDirectDMV2InboundWakeTest(raw)
		}()
		var once sync.Once
		closeLink := func() {
			t.Helper()
			once.Do(func() {
				_ = right.Close()
				_ = left.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("accepted handler did not join")
				}
			})
		}
		t.Cleanup(closeLink)
		intro := protocol.Frame{
			Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion,
			NodeType: string(domain.NodeTypeFull),
		}
		if capable {
			intro.Capabilities = []string{string(domain.CapDirectDMV2)}
		}
		proven, err := sessionv2.Dial(ctx, client, remote, intro,
			sessionv2.ExpectPeer(domain.PeerIdentityFromWire(author.Address)), sessionv2.DefaultTimeouts)
		if err != nil {
			t.Fatal(err)
		}
		peer, err := proven.Conn()
		if err != nil {
			t.Fatal(err)
		}
		if err := peer.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(peer)
		var initialMessage *protocol.Frame
		// The listener processes application frames only after its inbound
		// delivery setup returns. A pong therefore witnesses that boundary.
		ping, err := protocol.MarshalFrameLineBytes(protocol.Frame{Type: "ping"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Write(ping); err != nil {
			t.Fatal(err)
		}
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatalf("wait for accepted listener: %v", err)
			}
			var frame protocol.Frame
			if err := json.Unmarshal(line, &frame); err != nil {
				t.Fatal(err)
			}
			if frame.Type == "pong" {
				break
			}
			if frame.Type == "push_message" {
				if !realTimer || initialMessage != nil {
					t.Fatal("unexpected message while waiting for the listener")
				}
				initialMessage = &frame
			}
		}
		return link{raw: raw, reader: reader, initialMessage: initialMessage, close: closeLink}
	}
	connect := connectOutbound
	if inbound {
		connect = connectInbound
	}
	readFrame := func(reader *bufio.Reader) protocol.Frame {
		t.Helper()
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var frame protocol.Frame
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatal(err)
		}
		return frame
	}
	checkPayload := func(frame protocol.Frame) {
		t.Helper()
		item := frame.Item
		if frame.Type != "push_message" || item == nil || item.Body != original.Body ||
			item.ID != ref.ID || item.CreatedAt != original.CreatedAt || item.TTLSeconds != original.TTLSeconds {
			t.Fatal("writer did not preserve the stored authenticated envelope")
		}
	}

	if realTimer {
		loopCtx, stopLoop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			runtime.nodeService.RunDirectDMV2BootstrapWakeTest(loopCtx)
		}()
		stop := func() {
			t.Helper()
			stopLoop()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("bootstrap loop did not join")
			}
		}
		t.Cleanup(stop)

		var parked time.Time
		poll := time.NewTicker(20 * time.Millisecond)
		defer poll.Stop()
		for parked.IsZero() {
			select {
			case <-ctx.Done():
				t.Fatal("normal ticker did not park a delivery without a route")
			case <-poll.C:
				next, ok := runtime.nodeService.DirectDMV2NextAttemptForTest(ref)
				if ok && next.After(time.Now().Add(30*time.Second)) {
					parked = next
				}
			}
		}
		if row := readRow(); row != original || !chatlog.NeverEmitted(row.Metadata) {
			t.Fatal("parking consumed the outbox emission claim")
		}
		first := connect(true)
		if first.initialMessage != nil {
			checkPayload(*first.initialMessage)
		} else {
			for {
				frame := readFrame(first.reader)
				if frame.Type == "push_message" {
					checkPayload(frame)
					break
				}
			}
		}
		if !time.Now().Before(parked) {
			t.Fatal("scheduler sent only after the ordinary held poll became due")
		}
		stop()
		runtime.nodeService.WaitBackground()
		if onWire, known := chatlog.OnWire(readRow().Metadata); !known || !onWire {
			t.Fatal("flushed writer did not persist the sent mark")
		}
		first.close()
		return
	}

	tick := time.Now().UTC().Add(2 * time.Second)
	runTick(tick)
	parked, ok := runtime.nodeService.DirectDMV2NextAttemptForTest(ref)
	if !ok || !parked.After(tick.Add(2*time.Second)) {
		t.Fatal("missing route did not park the queued retry")
	}
	testClock.Store(tick.Add(time.Second).UnixNano())
	other := connect(false)
	if next, ok := runtime.nodeService.DirectDMV2NextAttemptForTest(ref); !ok || !next.Equal(parked) {
		t.Fatal("connection without capability woke the queue")
	}
	if row := readRow(); row != original || !chatlog.NeverEmitted(row.Metadata) {
		t.Fatal("connection without capability consumed the outbox emission claim")
	}
	other.close()

	wakeAt := tick.Add(2 * time.Second)
	var first link
	if staleArm {
		wakeAt = parked
		runtime.nodeService.SetDirectDMV2BeforeArmForTest(func() {
			first = connect(true)
		})
		runTick(wakeAt)
		runtime.nodeService.SetDirectDMV2BeforeArmForTest(nil)
	} else {
		testClock.Store(wakeAt.UnixNano())
		first = connect(true)
	}
	if next, ok := runtime.nodeService.DirectDMV2NextAttemptForTest(ref); !ok || !next.Equal(wakeAt) {
		t.Fatalf("publication did not leave the retry immediately due: %v", next)
	}
	started := first.raw.observeNextWrite()
	runTick(wakeAt)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("eligible original writer did not receive the dispatch")
	}
	// No read has begun on the far end of the unbuffered pipe. The writer
	// is in its actual socket write, and sent must still be unconfirmed.
	beforeFlush := readRow()
	if onWire, _ := chatlog.OnWire(beforeFlush.Metadata); onWire || chatlog.NeverEmitted(beforeFlush.Metadata) {
		t.Fatal("blocked writer either stamped sent or retained the pre-wire claim")
	}
	checkPayload(readFrame(first.reader))
	runtime.nodeService.WaitBackground()
	if onWire, known := chatlog.OnWire(readRow().Metadata); !known || !onWire {
		t.Fatal("flushed writer did not persist the sent mark")
	}
	first.close()

}

type retryWriteWitness struct {
	net.Conn
	local, remote net.Addr
	mu            sync.Mutex
	started       chan struct{}
}

func (c *retryWriteWitness) LocalAddr() net.Addr  { return c.local }
func (c *retryWriteWitness) RemoteAddr() net.Addr { return c.remote }

func (c *retryWriteWitness) observeNextWrite() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = make(chan struct{})
	return c.started
}

func (c *retryWriteWitness) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.started != nil {
		close(c.started)
		c.started = nil
	}
	c.mu.Unlock()
	return c.Conn.Write(p)
}
