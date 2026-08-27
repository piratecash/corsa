package deeplink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// A local socket exists because some platforms answer a clicked link by
// STARTING THE PROGRAM AGAIN with the URI on its command line (X11 and
// Wayland desktops do; macOS, Windows and Android all deliver into the
// running instance instead). A second process on one data directory is
// two nodes on one identity, so the newcomer hands the link to the
// instance that is already running and exits.
const (
	// socketPrefix names the socket in the node data directory: same
	// owner, same 0700 directory, so reachability is exactly "this
	// user". The scope that follows it is the node's own — two nodes
	// started from one directory on different ports are two identities
	// with two chat logs, and a link belongs to exactly one of them.
	socketPrefix = "deeplink-"

	// maxSocketPathBytes is the smallest sun_path across the platforms
	// this runs on (104 on darwin/BSD, 108 on Linux). A path longer
	// than that is silently truncated by the kernel, which would bind a
	// socket nobody can find — so it is refused with a real error.
	maxSocketPathBytes = 104

	// socketTimeout bounds both sides of one exchange. The exchange is
	// one short line each way over a local socket; anything slower is a
	// peer that is not answering.
	socketTimeout = 2 * time.Second

	ackOK = "ok"
)

// ErrAlreadyServing means another live instance owns the socket. The
// caller is the second process: it must not serve, and its own link (if
// any) belongs to the instance that answered.
var ErrAlreadyServing = errors.New("deep link socket: already served by a running instance")

// SocketPath is the local delivery address of ONE node: its data
// directory plus the scope that separates it from any other node living
// there (config.PortSuffix of its listen address, the same suffix its
// identity, peers and chatlog files carry).
func SocketPath(dataDir, scope string) string {
	if scope == "" {
		scope = "default"
	}
	return filepath.Join(dataDir, socketPrefix+scope+".sock")
}

// Forward hands raw to the instance listening on socketPath.
//
// It reports delivered=false with a nil error for the ordinary "nobody
// is listening" case (no socket, or a stale one left by a crash): the
// caller is then the only instance and owns the link itself. A non-nil
// error means somebody DID answer but the exchange failed — the link's
// fate is unknown, which is a different decision for the caller.
func Forward(ctx context.Context, socketPath, raw string) (bool, error) {
	link, err := Classify(raw)
	if err != nil {
		return false, err
	}

	dialer := net.Dialer{Timeout: socketTimeout}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		// No listener, or a socket file left behind by a crash.
		return false, nil
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(socketTimeout)); err != nil {
		return false, fmt.Errorf("deep link forward: set deadline: %w", err)
	}
	if _, err := conn.Write([]byte(link.URI + "\n")); err != nil {
		return false, fmt.Errorf("deep link forward: write: %w", err)
	}
	ack, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("deep link forward: read ack: %w", err)
	}
	if strings.TrimSpace(ack) != ackOK {
		return false, fmt.Errorf("deep link forward: peer refused the link")
	}
	return true, nil
}

// LocalListener serves the local delivery socket for the lifetime its
// owner gives it.
//
// Binding and consuming are separate steps on purpose. The bind is what
// makes a launch racing this one forward its link instead of starting a
// second node, so it has to happen BEFORE the data directory is opened —
// long before there is a window to hand a link to. Until a consumer
// attaches (Deliver), accepted links wait in the backlog.
type LocalListener struct {
	listener net.Listener
	path     string

	// queue is the single order links have: accepted at one end,
	// consumed at the other. A consumer that arrives late finds
	// everything still in it, in the order the socket took it.
	mu      sync.Mutex
	deliver func(Link)
	queue   []Link

	// flushMu lets exactly one goroutine walk the queue at a time, so
	// the consumer arriving and a link arriving cannot interleave into a
	// different order than the queue holds. Held across the consumer
	// call, which is why that call must not re-enter this listener.
	flushMu sync.Mutex
}

// Listen binds socketPath. Accepted links are held until Deliver names
// their consumer.
//
// A socket file whose owner is gone (a crash leaves one behind) is
// removed and rebound; a socket that still ANSWERS belongs to a live
// instance and yields ErrAlreadyServing.
func Listen(ctx context.Context, socketPath string) (*LocalListener, error) {
	if len(socketPath) >= maxSocketPathBytes {
		return nil, fmt.Errorf("deep link listen: socket path is %d bytes, kernel limit is %d", len(socketPath), maxSocketPathBytes-1)
	}
	// The bind happens before anything else opens the data directory, so
	// on a first run it may not exist yet. Same 0700 the node's own data
	// directory uses: the socket is reachable by its owner and nobody
	// else.
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, fmt.Errorf("deep link listen: create socket directory: %w", err)
	}

	listener, err := bindSocket(ctx, socketPath)
	if err != nil {
		return nil, err
	}
	// The data directory is already owner-only; this is the belt for the
	// case where it is not (a custom CORSA_DATA_DIR).
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("deep link listen: chmod socket: %w", err)
	}

	local := &LocalListener{listener: listener, path: socketPath}
	go local.accept(ctx)
	go func() {
		<-ctx.Done()
		_ = local.Close()
	}()
	return local, nil
}

// Deliver names the consumer of accepted links and hands it everything
// that arrived before it existed, oldest first. Called once, by the
// owner, as soon as there is something to deliver INTO; deliver runs on
// the accept goroutine, must not block, and must not call back into this
// listener.
func (l *LocalListener) Deliver(deliver func(Link)) {
	if deliver == nil {
		return
	}
	l.mu.Lock()
	l.deliver = deliver
	l.mu.Unlock()
	l.flush()
}

// hand queues one accepted link and moves the queue along. Queueing
// ALWAYS happens, consumer or not: a link that went straight to the
// consumer while older ones waited would arrive out of order.
func (l *LocalListener) hand(link Link) {
	l.mu.Lock()
	if len(l.queue) >= maxPendingLinks {
		l.mu.Unlock()
		log.Warn().Int("queued", maxPendingLinks).Msg("deep link socket queue full; dropping link")
		return
	}
	l.queue = append(l.queue, link)
	waiting := l.deliver == nil
	l.mu.Unlock()
	if waiting {
		return
	}
	l.flush()
}

// flush walks the queue in order. The consumer is called OUTSIDE the
// state lock — it reaches into the application — while flushMu keeps the
// walk single-file.
func (l *LocalListener) flush() {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	for {
		l.mu.Lock()
		if l.deliver == nil || len(l.queue) == 0 {
			l.mu.Unlock()
			return
		}
		link, deliver := l.queue[0], l.deliver
		l.queue = l.queue[1:]
		l.mu.Unlock()

		deliver(link)
	}
}

// Close stops the listener and removes the socket file.
func (l *LocalListener) Close() error {
	return l.listener.Close()
}

// Path is the address this listener is bound to.
func (l *LocalListener) Path() string { return l.path }

func bindSocket(ctx context.Context, socketPath string) (net.Listener, error) {
	config := net.ListenConfig{}
	listener, err := config.Listen(ctx, "unix", socketPath)
	if err == nil {
		return listener, nil
	}
	if !errors.Is(err, os.ErrExist) && !isAddrInUse(err) {
		return nil, fmt.Errorf("deep link listen: %w", err)
	}

	// Somebody bound this before us — or died holding it. Only the
	// answer to a dial tells the two apart.
	if conn, dialErr := net.DialTimeout("unix", socketPath, socketTimeout); dialErr == nil {
		_ = conn.Close()
		return nil, ErrAlreadyServing
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("deep link listen: remove stale socket: %w", err)
	}
	listener, err = config.Listen(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("deep link listen: rebind after stale socket: %w", err)
	}
	return listener, nil
}

func isAddrInUse(err error) bool {
	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		return strings.Contains(sysErr.Err.Error(), "address already in use")
	}
	return strings.Contains(err.Error(), "address already in use")
}

// accept serves connections ONE AT A TIME: every exchange is a single
// short line under a deadline, and a per-connection goroutine would only
// add an unbounded spawn point on a socket a local process can hammer.
func (l *LocalListener) accept(ctx context.Context) {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Warn().Err(err).Str("socket", l.path).Msg("deep link socket accept failed")
			continue
		}
		l.serve(conn)
	}
}

func (l *LocalListener) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(socketTimeout)); err != nil {
		log.Warn().Err(err).Msg("deep link socket deadline failed")
		return
	}
	// Bounded before anything is parsed: the cap is the URI cap plus the
	// newline that terminates it.
	reader := bufio.NewReader(&limitedConn{Reader: conn, remaining: MaxURIBytes + 1})
	raw, err := reader.ReadString('\n')
	if raw == "" {
		if errors.Is(err, io.EOF) {
			// A liveness probe, not a failure: bindSocket dials the
			// address to tell a socket file left by a crash from one a
			// live instance still answers on, and closes without
			// writing anything.
			log.Debug().Msg("deep link socket probed")
			return
		}
		log.Warn().Err(err).Msg("deep link socket read failed")
		return
	}

	link, err := Classify(raw)
	if err != nil {
		log.Warn().Err(err).Msg("deep link socket got an unroutable URI")
		_, _ = conn.Write([]byte("err\n"))
		return
	}
	if _, err := conn.Write([]byte(ackOK + "\n")); err != nil {
		log.Warn().Err(err).Msg("deep link socket ack failed")
		// The link is good and the sender is about to exit believing
		// otherwise — delivering it is still the better half of a bad
		// outcome, so fall through.
	}
	log.Info().Str("kind", link.Kind.String()).Msg("deep link received from a second launch")
	l.hand(link)
}

// limitedConn caps how much one connection may hand the parser.
type limitedConn struct {
	Reader    net.Conn
	remaining int
}

func (c *limitedConn) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		return 0, fmt.Errorf("deep link socket: input exceeds %d bytes", MaxURIBytes)
	}
	if len(p) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.Reader.Read(p)
	c.remaining -= n
	return n, err
}
