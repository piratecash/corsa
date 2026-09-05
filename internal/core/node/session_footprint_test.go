package node

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// session_footprint_test.go measures part of the number 13-measurements.md §2
// calls blocking: what ONE session costs.
//
// It is blocking because the overlay's whole premise turns on it. A structured
// overlay wants 32–64 structural sessions against today's eight outgoing
// slots, so if a session is expensive the migration spends more than it saves
// and the right answer is to stop before D1 rather than to discover it at D5.
//
// §2 names THREE costs, and this file answers two of them:
//
//   - MEMORY — measured, by holding real sessions across a GC. It is the fixed
//     part only: the buffers a connection allocates by existing, before it
//     carries a frame;
//   - DESCRIPTORS — measured separately, over real loopback sockets, because
//     the memory measurement runs over net.Pipe and net.Pipe opens no
//     descriptor at all. A pipe-based measurement that claimed to cover
//     descriptors would be claiming a number it structurally cannot see;
//   - BACKGROUND TRAFFIC of keeping a session alive — NOT measured here and
//     not measurable here. It needs two nodes exchanging over time, which is a
//     load harness this tree does not have. It stays an open item of §8.6
//     rather than an assumed zero.
//
// Reference: docs/refactoring/dht/13-measurements.md §2 ("стоимость одной
// сессии"), §5, §8.4.

// sessionFootprintCount is enough sessions for the per-session figure to
// dominate the fixed cost of the measurement itself, and few enough to keep
// the pipes and goroutines manageable.
const sessionFootprintCount = 200

// retainedBytes measures what building something keeps alive, by reading the
// heap on both sides of a forced GC with the result held across the second
// one. A benchmark would answer a different question — what the construction
// ALLOCATES, garbage included — and the question here is what stays.
func retainedBytes(build func() any) uint64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	built := build()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(built)

	if after.HeapAlloc < before.HeapAlloc {
		return 0
	}
	return after.HeapAlloc - before.HeapAlloc
}

// TestSessionDescriptorCost measures the second of §2's three costs.
//
// It runs over REAL loopback sockets rather than net.Pipe, because a pipe
// consumes no descriptor: the memory test above could not have seen this
// number even in principle, and reporting it from there would have been a
// claim about something never observed.
//
// The expected answer is one descriptor per connection on each side, and the
// value of measuring it is not the surprise — it is that 64 structural
// sessions can then be stated as 64 descriptors against the process limit
// instead of being assumed.
func TestSessionDescriptorCost(t *testing.T) {
	if testing.Short() {
		t.Skip("opens real loopback sockets")
	}

	// Measured, not predicted: the descriptor directory either answers or it
	// does not, and the skip says which — rather than guessing from the OS
	// name, which is how a platform that CAN answer ends up never being asked.
	before, err := openDescriptorCount()
	if err != nil {
		t.Skipf("this platform does not expose a descriptor count: %v", err)
	}

	const connections = 50
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	// Both ends are RETAINED until after the measurement, and that is the whole
	// correctness of this test rather than tidiness. A receive that dropped the
	// accepted conn on the floor would leave the server side unreferenced: the
	// runtime is then free to finalize it — closing its descriptor — at any
	// moment, so the number this test reports would depend on when a GC
	// happened to run.
	var (
		dialled  = make([]net.Conn, 0, connections)
		served   = make([]net.Conn, 0, connections)
		accepted = make(chan net.Conn, connections)
	)
	t.Cleanup(func() {
		for _, conn := range dialled {
			_ = conn.Close()
		}
		for _, conn := range served {
			_ = conn.Close()
		}
	})

	go func() {
		for range connections {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			accepted <- conn
		}
	}()

	for i := range connections {
		conn, dialErr := net.Dial("tcp", listener.Addr().String())
		if dialErr != nil {
			t.Fatalf("dial %d: %v", i, dialErr)
		}
		dialled = append(dialled, conn)
	}
	for range connections {
		select {
		case conn := <-accepted:
			served = append(served, conn)
		case <-time.After(5 * time.Second):
			t.Fatal("the listener did not accept every connection")
		}
	}

	after, err := openDescriptorCount()
	if err != nil {
		t.Fatalf("descriptor count became unreadable mid-test: %v", err)
	}
	// Both slices are alive across the reading, and this says so to the
	// compiler as well as to the reader: the measurement is only meaningful
	// while every socket it counts is still held.
	runtime.KeepAlive(dialled)
	runtime.KeepAlive(served)

	// Both ends live in this process, so each connection costs two descriptors
	// here and one on a real node. Asserting the range rather than a value
	// keeps the test from failing on an unrelated descriptor the runtime opens
	// while it runs.
	opened := after - before
	if opened < connections || opened > 3*connections {
		t.Fatalf("%d connections opened %d descriptors: expected about two per connection (both ends are local)",
			connections, opened)
	}
	t.Logf("descriptors: %d for %d loopback connections (%.2f per connection, both ends local ⇒ ~1 per session on a node)",
		opened, connections, float64(opened)/float64(connections))
}

// openDescriptorCount reports how many descriptors this process holds, or why
// it cannot be asked.
func openDescriptorCount() (int, error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	// The directory handle opened to read this is itself counted; it is closed
	// by ReadDir before returning, and both readings pay the same cost anyway.
	return len(entries), nil
}

// TestSessionFixedFootprint measures what a live connection costs IN MEMORY
// before it carries anything, and pins the two claims a budget is built on.
//
// Memory only. It runs over net.Pipe, which opens no descriptor and sends
// nothing, so it says nothing about the other two costs §2 names — see
// TestSessionDescriptorCost and the open item in §8.6.
//
// The assertions are deliberately about SHAPE, not about a byte figure. A
// hard number would pin the allocator and the Go version; what must not change
// silently is that the cost is dominated by fixed-size buffers whose sizes are
// constants in this tree, and that it is therefore predictable per session
// rather than a function of the network.
func TestSessionFixedFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("opens hundreds of pipes and goroutines")
	}

	type liveSession struct {
		core    *netcore.NetCore
		session *peerSession
		client  net.Conn
	}

	sessions := make([]liveSession, 0, sessionFootprintCount)
	t.Cleanup(func() {
		for _, live := range sessions {
			live.core.Close()
			_ = live.client.Close()
		}
	})

	retained := retainedBytes(func() any {
		for i := range sessionFootprintCount {
			clientPipe, serverPipe := net.Pipe()
			peer := domain.PeerIdentityFromWire(fmt.Sprintf("%040x", i+1))
			address := domain.PeerAddress(fmt.Sprintf("10.10.%d.%d:64646", i/256, i%256))
			declarations := datagramPeerDeclarations()
			core := netcore.New(domain.ConnID(20000+i), serverPipe, netcore.Outbound, netcore.Options{
				Address:         address,
				Identity:        peer,
				Caps:            []domain.Capability{domain.CapMeshDatagramV1},
				ProtocolVersion: domain.ProtocolVersion(30),
				Declarations:    &declarations,
			})
			sessions = append(sessions, liveSession{
				core:   core,
				client: clientPipe,
				// The upper half of a connection: the node's own per-peer
				// record with its send and inbox channels, which is where the
				// larger of the two fixed buffers lives.
				session: &peerSession{
					address:      address,
					connID:       domain.ConnID(20000 + i),
					peerIdentity: peer,
					sendCh:       make(chan peerSendItem, peerSessionSendBuffer),
					inboxCh:      make(chan protocol.Frame, peerSessionInboxBuffer),
					errCh:        make(chan error, 1),
					authOK:       true,
					declarations: declarations.Clone(),
				},
			})
		}
		return sessions
	})

	perSession := retained / sessionFootprintCount

	// The channel buffers alone, from the constants in this tree. Everything
	// else a session holds — the read buffer, two goroutine stacks, the
	// handshake declarations — sits on top, so the measured figure must exceed
	// this and a figure below it would mean a buffer stopped being allocated.
	bufferFloor := uint64(peerSessionInboxBuffer)*domain.SizeOfAll(protocol.Frame{}) +
		uint64(peerSessionSendBuffer)*domain.SizeOfAll(peerSendItem{})
	if perSession < bufferFloor {
		t.Fatalf("a session measured %d B, below the %d B its fixed channel buffers alone require: a buffer this budget assumes is no longer being allocated",
			perSession, bufferFloor)
	}

	// The inbox is the dominant term by construction — 256 slots of a large
	// frame struct — and the budget for the overlay's extra sessions is sized
	// against that. If it ever stops dominating, the per-session cost has a
	// new shape and the multiplication in the roadmap needs redoing.
	inboxBytes := uint64(peerSessionInboxBuffer) * domain.SizeOfAll(protocol.Frame{})
	if inboxBytes*2 < bufferFloor {
		t.Fatalf("the inbox buffer is no longer the dominant fixed cost of a session (%d B of %d B): the per-session figure in 13-measurements.md was derived from it",
			inboxBytes, bufferFloor)
	}

	t.Logf("session fixed cost: %d B measured per session over %d sessions (channel-buffer floor %d B, of which inbox %d B)",
		perSession, sessionFootprintCount, bufferFloor, inboxBytes)
	t.Logf("  %d structural sessions would cost %.2f MB",
		64, float64(perSession*64)/(1024*1024))
}
