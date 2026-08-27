package deeplink_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// testScope stands for the listen-port suffix every node-local file
// carries (config.PortSuffix), so these tests exercise the same shape a
// node uses.
const testScope = "64646"

// socketDir is deliberately NOT t.TempDir: the kernel truncates a unix
// socket path past ~104 bytes, and the macOS temp root alone eats most
// of that budget.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "corsa-dl-")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// received drains one delivered link with a bound, so a broken listener
// fails the test instead of hanging it.
func received(t *testing.T, links chan deeplink.Link) deeplink.Link {
	t.Helper()
	select {
	case link := <-links:
		return link
	case <-time.After(5 * time.Second):
		t.Fatal("no link delivered")
		return deeplink.Link{}
	}
}

// TestForwardReachesTheRunningInstance is the whole point of the socket:
// a second launch hands its link over and the first instance receives it
// classified.
func TestForwardReachesTheRunningInstance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := deeplink.SocketPath(socketDir(t), testScope)
	links := make(chan deeplink.Link, 1)
	listener, err := deeplink.Listen(ctx, path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	listener.Deliver(func(link deeplink.Link) { links <- link })

	contact := newContactLink(t)
	delivered, err := deeplink.Forward(ctx, path, contact)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if !delivered {
		t.Fatal("forward reported the link undelivered")
	}

	link := received(t, links)
	if link.Kind != deeplink.KindContact {
		t.Errorf("kind = %q, want %q", link.Kind, deeplink.KindContact)
	}
	if link.URI != contact {
		t.Errorf("URI changed in transit:\n got %s\nwant %s", link.URI, contact)
	}
}

// TestForwardWithNobodyListening: the ordinary cold start. No error —
// the caller IS the instance and owns its link.
func TestForwardWithNobodyListening(t *testing.T) {
	path := deeplink.SocketPath(socketDir(t), testScope)

	delivered, err := deeplink.Forward(context.Background(), path, newContactLink(t))
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if delivered {
		t.Error("forward claims delivery with no listener")
	}
}

// TestForwardRejectsJunkBeforeDialing: a URI that cannot be routed is
// the caller's own mistake and must not reach the socket at all.
func TestForwardRejectsJunkBeforeDialing(t *testing.T) {
	path := deeplink.SocketPath(socketDir(t), testScope)

	if _, err := deeplink.Forward(context.Background(), path, "https://example.org"); !errors.Is(err, deeplink.ErrMalformed) {
		t.Fatalf("err = %v, want %v", err, deeplink.ErrMalformed)
	}
}

// TestListenRebindsOverAStaleSocket: a crash leaves the socket file
// behind, and the next start must not be locked out by a file whose
// owner is gone.
func TestListenRebindsOverAStaleSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := socketDir(t)
	path := deeplink.SocketPath(dir, testScope)

	stale, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("stale listener: %v", err)
	}
	// Close WITHOUT unlinking — exactly what a killed process leaves.
	unixListener, ok := stale.(*net.UnixListener)
	if !ok {
		t.Fatalf("unexpected listener type %T", stale)
	}
	unixListener.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale listener: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stale socket file should still exist: %v", err)
	}

	links := make(chan deeplink.Link, 1)
	listener, err := deeplink.Listen(ctx, path)
	if err != nil {
		t.Fatalf("listen over stale socket: %v", err)
	}
	defer func() { _ = listener.Close() }()
	listener.Deliver(func(link deeplink.Link) { links <- link })

	delivered, err := deeplink.Forward(ctx, path, newContactLink(t))
	if err != nil || !delivered {
		t.Fatalf("forward after rebind: delivered=%v err=%v", delivered, err)
	}
	received(t, links)
}

// TestListenRefusesToStealALiveSocket: two live instances must not both
// claim the address, or a link would reach a coin toss.
func TestListenRefusesToStealALiveSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := deeplink.SocketPath(socketDir(t), testScope)
	first, err := deeplink.Listen(ctx, path)
	if err != nil {
		t.Fatalf("first listen: %v", err)
	}
	defer func() { _ = first.Close() }()

	if _, err := deeplink.Listen(ctx, path); !errors.Is(err, deeplink.ErrAlreadyServing) {
		t.Fatalf("second listen err = %v, want %v", err, deeplink.ErrAlreadyServing)
	}
}

// TestListenRefusesAnUnbindablePath: a path the kernel would truncate
// binds a socket nobody can find, so it fails loudly instead.
func TestListenRefusesAnUnbindablePath(t *testing.T) {
	long := filepath.Join("/tmp", strings.Repeat("d", 120), "deeplink.sock")
	if _, err := deeplink.Listen(context.Background(), long); err == nil {
		t.Fatal("an over-long socket path was accepted")
	}
}

// TestListenDropsJunkAtTheDoor: whatever a local process writes, only a
// routable URI reaches the application.
func TestListenDropsJunkAtTheDoor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := deeplink.SocketPath(socketDir(t), testScope)
	links := make(chan deeplink.Link, 2)
	listener, err := deeplink.Listen(ctx, path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	listener.Deliver(func(link deeplink.Link) { links <- link })

	for _, junk := range []string{
		"https://example.org\n",
		"corsa:" + strings.Repeat("a", deeplink.MaxURIBytes) + "\n",
		"\n",
		// A connection that says nothing at all — what the stale-socket
		// probe in Listen does.
		"",
	} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if junk != "" {
			if _, err := conn.Write([]byte(junk)); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		_ = conn.Close()
	}

	// A good link behind the junk proves the listener is still serving.
	if _, err := deeplink.Forward(ctx, path, newContactLink(t)); err != nil {
		t.Fatalf("forward after junk: %v", err)
	}
	link := received(t, links)
	if link.Kind != deeplink.KindContact {
		t.Fatalf("first delivered link was %q — junk got through", link.Kind)
	}
}

// TestListenStopsWithItsContext: the socket is owned by the run, and a
// cancelled run leaves no listener behind.
func TestListenStopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	path := deeplink.SocketPath(socketDir(t), testScope)

	if _, err := deeplink.Listen(ctx, path); err != nil {
		t.Fatalf("listen: %v", err)
	}
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
		if err != nil {
			return // the listener is gone, as asked
		}
		_ = conn.Close()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("listener still answering after its context was cancelled")
}

// TestListenHoldsLinksUntilThereIsSomewhereToPutThem is why binding and
// consuming are separate: the socket is claimed before the data
// directory is opened, so a link can arrive seconds before a window
// exists — and must not be dropped for being early.
func TestListenHoldsLinksUntilThereIsSomewhereToPutThem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := deeplink.SocketPath(socketDir(t), testScope)
	listener, err := deeplink.Listen(ctx, path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	contact := newContactLink(t)
	delivered, err := deeplink.Forward(ctx, path, contact)
	if err != nil || !delivered {
		t.Fatalf("forward before a consumer: delivered=%v err=%v", delivered, err)
	}

	links := make(chan deeplink.Link, 1)
	listener.Deliver(func(link deeplink.Link) { links <- link })

	if got := received(t, links); got.URI != contact {
		t.Errorf("backlog delivered %q, want %q", got.URI, contact)
	}
}

// TestListenCreatesTheSocketDirectory: the claim happens before anything
// else opens the data directory, and on a first run nothing has made it
// yet.
func TestListenCreatesTheSocketDirectory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := filepath.Join(socketDir(t), "corsa")
	listener, err := deeplink.Listen(ctx, deeplink.SocketPath(dataDir, testScope))
	if err != nil {
		t.Fatalf("listen into a missing directory: %v", err)
	}
	defer func() { _ = listener.Close() }()

	info, err := os.Stat(dataDir)
	if err != nil {
		t.Fatalf("stat data dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("data dir mode = %04o, want 0700", perm)
	}
}

// TestSocketPathSeparatesNodesInOneDirectory: two nodes started from one
// data directory on different ports are two identities with two chat
// logs — every other node-local file carries the port, and a shared
// socket would hand one node's link to the other.
func TestSocketPathSeparatesNodesInOneDirectory(t *testing.T) {
	dir := "/tmp/corsa-data"

	first := deeplink.SocketPath(dir, "64646")
	second := deeplink.SocketPath(dir, "64647")

	if first == second {
		t.Fatalf("both nodes bind %s", first)
	}
	if !strings.Contains(first, "64646") || !strings.Contains(second, "64647") {
		t.Errorf("paths do not name their node: %s, %s", first, second)
	}
	if got := deeplink.SocketPath(dir, ""); got == filepath.Join(dir, ".sock") || !strings.Contains(got, "default") {
		t.Errorf("an unscoped path must still be a named one, got %s", got)
	}
}

// TestListenKeepsTheOrderLinksArrivedIn: the queue is the only order
// there is, so a consumer attaching late gets the backlog oldest-first —
// the last link a user opened must be the last status they see.
func TestListenKeepsTheOrderLinksArrivedIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := deeplink.SocketPath(socketDir(t), testScope)
	listener, err := deeplink.Listen(ctx, path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	sent := []string{"corsa:group/one", "corsa:group/two", "corsa:group/three"}
	for _, raw := range sent {
		delivered, err := deeplink.Forward(ctx, path, raw)
		if err != nil || !delivered {
			t.Fatalf("forward %q: delivered=%v err=%v", raw, delivered, err)
		}
	}

	links := make(chan deeplink.Link, len(sent))
	listener.Deliver(func(link deeplink.Link) { links <- link })

	for _, want := range sent {
		if got := received(t, links); got.URI != want {
			t.Fatalf("out of order: got %q, want %q", got.URI, want)
		}
	}
}

// TestClosedListenerStopsTakingLinks is the shutdown contract: once the
// socket is closed, a launch is told nobody is listening and opens the
// link itself, instead of being acknowledged by an instance that is on
// its way out.
func TestClosedListenerStopsTakingLinks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := deeplink.SocketPath(socketDir(t), testScope)
	listener, err := deeplink.Listen(ctx, path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listener.Deliver(func(deeplink.Link) {})

	if err := listener.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	delivered, err := deeplink.Forward(ctx, path, newContactLink(t))
	if err != nil {
		t.Fatalf("forward after close: %v", err)
	}
	if delivered {
		t.Fatal("a closed listener still took a link")
	}
}
