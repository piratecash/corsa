package node

import (
	"sync"
	"time"
)

// connRateLimiter enforces per-IP connection rate limiting at the TCP accept
// level. Each IP gets a sliding-window counter that tracks connection attempts.
// When the rate exceeds the configured threshold, new connections from that IP
// are rejected immediately — before any protocol parsing or resource allocation.
//
// This prevents SYN flood and connection exhaustion attacks where a single IP
// opens hundreds of connections per second to consume file descriptors and
// goroutines. The approach follows established P2P network patterns: limit per
// source before allocating any per-connection state.
type connRateLimiter struct {
	mu      sync.Mutex
	windows map[string]*connWindow

	// maxPerWindow is the maximum number of connections allowed from a single
	// IP within one window period. Connections beyond this limit are rejected.
	maxPerWindow int

	// windowSize is the duration of each sliding window.
	windowSize time.Duration
}

// connWindow tracks connection attempts from a single IP address using a
// sliding window counter. When the current window expires, the count resets.
type connWindow struct {
	count       int
	windowStart time.Time
}

// defaultConnRateLimit is the maximum connections per IP per window.
// 10 connections per 10 seconds is generous for legitimate peers (which
// typically maintain 1-2 persistent connections) while blocking floods.
const defaultConnRateLimit = 10

// defaultConnRateWindow is the sliding window duration for connection rate
// limiting. 10 seconds provides a good balance between detection speed and
// tolerance for legitimate reconnection bursts.
const defaultConnRateWindow = 10 * time.Second

// maxConnPerIP is the hard cap on simultaneous connections from a single IP.
// Even within rate limits, no single IP should hold more than this many
// concurrent connections. Prevents resource exhaustion from IPs that connect
// slowly but never disconnect.
const maxConnPerIP = 8

func newConnRateLimiter() *connRateLimiter {
	return &connRateLimiter{
		windows:      make(map[string]*connWindow),
		maxPerWindow: defaultConnRateLimit,
		windowSize:   defaultConnRateWindow,
	}
}

// allowConnect checks whether a new connection from the given IP should be
// permitted based on the sliding window rate. Returns true if allowed,
// false if the rate has been exceeded.
func (cl *connRateLimiter) allowConnect(ip string) bool {
	if ip == "" {
		return true
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	now := time.Now()
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

// cleanup removes stale window entries that haven't been updated recently.
// Called periodically to prevent unbounded map growth from abandoned IPs.
func (cl *connRateLimiter) cleanup(maxAge time.Duration) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
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
// (send_message, publish_notice, announce_peer, etc.). Relay frames have
// their own dedicated limiter (relayRateLimiter); this covers everything else.
//
// Without this, an authenticated peer could flood the node with thousands of
// send_message or publish_notice frames per second, consuming CPU for JSON
// parsing, signature verification, and fan-out — even though each individual
// frame passes all validation checks.
type commandRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*cmdBucket
}

type cmdBucket struct {
	tokens     float64
	lastRefill time.Time
}

// cmdBurstPerConn is the maximum command burst from a single connection.
const cmdBurstPerConn = 100

// cmdRefillRate is commands per second refill rate.
// 30 commands/s is well above normal peer behavior (~5/s sustained).
const cmdRefillRate = 30.0

func newCommandRateLimiter() *commandRateLimiter {
	return &commandRateLimiter{
		buckets: make(map[string]*cmdBucket),
	}
}

// allowCommand checks whether a command from the identified connection should
// be permitted. The key should uniquely identify the connection (e.g.
// RemoteAddr().String()).
func (rl *commandRateLimiter) allowCommand(connKey string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[connKey]
	if !ok {
		b = &cmdBucket{
			tokens:     cmdBurstPerConn,
			lastRefill: now,
		}
		rl.buckets[connKey] = b
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * cmdRefillRate
	if b.tokens > cmdBurstPerConn {
		b.tokens = cmdBurstPerConn
	}
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// removeConn removes the bucket for a closed connection.
func (rl *commandRateLimiter) removeConn(connKey string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.buckets, connKey)
}

// cleanup removes stale buckets for connections that haven't been seen recently.
func (rl *commandRateLimiter) cleanup(maxAge time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	for key, b := range rl.buckets {
		if b.lastRefill.Before(cutoff) {
			delete(rl.buckets, key)
		}
	}
}
