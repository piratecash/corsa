package node

import (
	"sync"
	"time"
)

// connRateLimiter enforces per-IP connection rate limiting at the TCP accept
// level. Each IP gets a fixed-window counter that tracks connection attempts:
// the window opens at the first attempt and the count starts over at the first
// attempt once the window has run its full length. When the count exceeds the
// configured threshold, new connections from that IP are rejected immediately —
// before any protocol parsing or resource allocation.
//
// This prevents SYN flood and connection exhaustion attacks where a single IP
// opens hundreds of connections per second to consume file descriptors and
// goroutines. The approach follows established P2P network patterns: limit per
// source before allocating any per-connection state.
//
// Time is read only through the injected clock (the wall clock in
// production), so window expiry and stale-entry cleanup can be driven
// deterministically in tests.
type connRateLimiter struct {
	mu      sync.Mutex
	windows map[string]*connWindow

	// maxPerWindow is the maximum number of connections allowed from a single
	// IP within one window period. Connections beyond this limit are rejected.
	maxPerWindow int

	// windowSize is the duration of each window.
	windowSize time.Duration

	// clock is set once in newConnRateLimiter and never written again,
	// which is why it needs no protection from mu.
	clock func() time.Time
}

// connWindow tracks connection attempts from a single IP address in one fixed
// window. When the current window expires, the count resets.
type connWindow struct {
	count       int
	windowStart time.Time
}

// defaultConnRateLimit is the maximum connections per IP per window.
// 10 connections per 10 seconds is generous for legitimate peers (which
// typically maintain 1-2 persistent connections) while blocking floods.
const defaultConnRateLimit = 10

// defaultConnRateWindow is the fixed window duration for connection rate
// limiting. 10 seconds provides a good balance between detection speed and
// tolerance for legitimate reconnection bursts.
const defaultConnRateWindow = 10 * time.Second

// maxConnPerIP is the hard cap on simultaneous connections from a single IP.
// Even within rate limits, no single IP should hold more than this many
// concurrent connections. Prevents resource exhaustion from IPs that connect
// slowly but never disconnect.
const maxConnPerIP = 8

// newConnRateLimiter builds a limiter whose windows expire and age by clock.
// Production passes time.Now; a nil clock falls back to it, the same as the
// package's other clock-driven structures.
func newConnRateLimiter(clock func() time.Time) *connRateLimiter {
	if clock == nil {
		clock = time.Now
	}
	return &connRateLimiter{
		clock:        clock,
		windows:      make(map[string]*connWindow),
		maxPerWindow: defaultConnRateLimit,
		windowSize:   defaultConnRateWindow,
	}
}

// allowConnect checks whether a new connection from the given IP should be
// permitted based on the fixed window rate. Returns true if allowed,
// false if the rate has been exceeded.
func (cl *connRateLimiter) allowConnect(ip string) bool {
	if ip == "" {
		return true
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	now := cl.clock()
	w, ok := cl.windows[ip]
	if !ok {
		cl.windows[ip] = &connWindow{count: 1, windowStart: now}
		return true
	}

	// Reset window if expired.
	if now.Sub(w.windowStart) >= cl.windowSize {
		w.count = 1
		w.windowStart = now
		return true
	}

	w.count++
	return w.count <= cl.maxPerWindow
}

// sweepIdle drops every window that started more than windowSize ago.
// bootstrapLoop runs it on every tick, which is what bounds the map: without
// it each distinct source IP that ever reached the listener would keep an
// entry for the life of the process.
//
// The horizon is the window itself, read from the same field allowConnect
// resets on, so it cannot be configured below it. Dropping such a window
// grants nothing: allowConnect would open a fresh window with a count of 1 on
// that IP's next attempt anyway (it resets at elapsed >= windowSize), which is
// exactly what a missing entry gives it. A window exactly windowSize old is
// kept (strict Before); its next attempt resets it, so keeping it changes
// nothing either.
func (cl *connRateLimiter) sweepIdle() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cutoff := cl.clock().Add(-cl.windowSize)
	for ip, w := range cl.windows {
		if w.windowStart.Before(cutoff) {
			delete(cl.windows, ip)
		}
	}
}

// ---------------------------------------------------------------------------
// Per-peer command rate limiter for non-relay frames
// ---------------------------------------------------------------------------

// commandRateLimiter enforces per-connection rate limits on command frames
// (push_message, push_notice, announce_peer, etc.). Relay frames have
// their own dedicated limiter (relayRateLimiter); this covers everything else.
//
// Without this, an authenticated peer could flood the node with thousands of
// push_message or push_notice frames per second, consuming CPU for JSON
// parsing, signature verification, and fan-out — even though each individual
// frame passes all validation checks.
//
// Time is read only through the injected clock (the wall clock in
// production), so refill and stale-bucket cleanup can be driven
// deterministically in tests.
type commandRateLimiter struct {
	buckets *tokenBucketSet[string]
}

// cmdBurstPerConn is the maximum command burst from a single connection.
const cmdBurstPerConn = 100

// cmdRefillRate is commands per second refill rate.
// 30 commands/s is well above normal peer behavior (~5/s sustained).
const cmdRefillRate = 30.0

// cmdLimiterIdleAge is the inactivity window after which sweepIdle (run on
// every bootstrapLoop tick) evicts a per-connection command bucket. Every
// bucket has an owner that removes it when its connection ends — the inbound
// read loop (removeConn) or the outbound session's goroutine
// (dropOutboundControlFrameBucket) — so the sweep is only the backstop for a
// bucket a future path forgets to drop. It must stay at least as long as a drained bucket takes to
// refill (cmdBurstPerConn / cmdRefillRate ≈ 3.3 s): an evicted key comes back
// with a full bucket, so a shorter horizon would be a refill shortcut for a
// connection that is still open.
const cmdLimiterIdleAge = 5 * time.Minute

// newCommandRateLimiter builds a limiter whose buckets refill and age by
// clock. Production passes time.Now; nil falls back to it (see
// newTokenBucketSet).
func newCommandRateLimiter(clock func() time.Time) *commandRateLimiter {
	return &commandRateLimiter{
		buckets: newTokenBucketSet[string](tokenBucketPolicy{
			burst:           cmdBurstPerConn,
			refillPerSecond: cmdRefillRate,
			idleAge:         cmdLimiterIdleAge,
		}, clock),
	}
}

// allowCommand checks whether a command from the identified connection should
// be permitted. The key should uniquely identify the connection (e.g.
// RemoteAddr().String()).
func (rl *commandRateLimiter) allowCommand(connKey string) bool {
	return rl.buckets.take(connKey, 1)
}

// removeConn removes the bucket for a closed connection at once, so it does
// not wait for sweepIdle.
func (rl *commandRateLimiter) removeConn(connKey string) {
	rl.buckets.forget(connKey)
}

// sweepIdle evicts buckets idle for longer than the limiter's idle age.
func (rl *commandRateLimiter) sweepIdle() {
	rl.buckets.sweepIdle()
}
