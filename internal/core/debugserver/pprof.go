// Package debugserver hosts the opt-in Go pprof profiling endpoint.
//
// The server is OFF unless CORSA_PPROF_ADDR (config.Node.PprofAddr) is
// set, and it refuses to bind anywhere but a loopback address — the
// pprof surface exposes process internals (heap/CPU profiles, goroutine
// dumps) and must never be reachable from the network. It is meant for
// diagnosing a single node during an active investigation:
//
//	CORSA_PPROF_ADDR=127.0.0.1:6060 ./corsa-node
//	go tool pprof http://127.0.0.1:6060/debug/pprof/heap
//	go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30
package debugserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// Start launches the pprof HTTP server on addr in a background
// goroutine and returns a shutdown function. When addr is empty it is
// a no-op (returns a nil-safe shutdown). A non-loopback addr is
// rejected with an error so a misconfiguration can never expose the
// debug surface to the network.
//
// The returned shutdown function is safe to call even when the server
// did not start (empty addr / bind error).
func Start(addr string) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }
	if addr == "" {
		return noop, nil
	}
	if err := preflightLoopback(addr); err != nil {
		return noop, err
	}

	// Opt-in contention profiling. The named /debug/pprof/mutex and
	// /debug/pprof/block endpoints are already served by pprof.Index
	// below, but they return EMPTY profiles unless the runtime sampling
	// rates are non-zero — which is why peerMu contention under a
	// route-update storm was invisible to pprof. Enable from env.
	configureContentionProfiling()

	// Dedicated mux — do NOT use http.DefaultServeMux, which a stray
	// import elsewhere could pollute. Register only the pprof handlers.
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return noop, err
	}

	// Authoritative guard: verify the ACTUAL bound address, not the
	// requested string. "localhost" (and any hostname) resolves through
	// the system resolver / hosts file, so the only way to be sure the
	// listener really landed on a loopback interface is to inspect
	// ln.Addr() after the bind. If it did not, close immediately and
	// refuse — the pprof surface must never be reachable off-host.
	if err := verifyBoundLoopback(ln); err != nil {
		_ = ln.Close()
		return noop, err
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Warn().
			Str("addr", ln.Addr().String()).
			Msg("pprof debug server enabled (CORSA_PPROF_ADDR) — loopback only; disable in production")
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error().Err(serveErr).Msg("pprof debug server exited with error")
		}
	}()

	return srv.Shutdown, nil
}

// configureContentionProfiling enables the mutex and block profiles
// from env when the pprof server is on. Both default to OFF (sampling
// rate 0) because they add per-operation runtime overhead; an operator
// diagnosing lock contention (e.g. peerMu under a route-update storm)
// turns them on explicitly:
//
//	CORSA_PPROF_MUTEX_FRACTION=5    # sample ~1/5 of mutex contention events
//	CORSA_PPROF_BLOCK_RATE=10000    # sample a blocking event ~per 10us blocked
//	go tool pprof http://127.0.0.1:6060/debug/pprof/mutex
//	go tool pprof http://127.0.0.1:6060/debug/pprof/block
//
// Both are process-global runtime knobs (runtime.SetMutexProfileFraction
// / runtime.SetBlockProfileRate); we only ever turn them ON here, never
// reset to 0, so enabling pprof mid-investigation cannot silently clear
// a rate an operator set another way.
func configureContentionProfiling() {
	if frac := envPositiveInt("CORSA_PPROF_MUTEX_FRACTION"); frac > 0 {
		runtime.SetMutexProfileFraction(frac)
		log.Warn().
			Int("fraction", frac).
			Msg("pprof mutex profiling enabled (CORSA_PPROF_MUTEX_FRACTION) — adds runtime overhead")
	}
	if rate := envPositiveInt("CORSA_PPROF_BLOCK_RATE"); rate > 0 {
		runtime.SetBlockProfileRate(rate)
		log.Warn().
			Int("rate_ns", rate).
			Msg("pprof block profiling enabled (CORSA_PPROF_BLOCK_RATE) — adds runtime overhead")
	}
}

// envPositiveInt parses a strictly-positive integer from the named env
// var, returning 0 (treated as "unset / disabled") on absence or any
// parse error.
func envPositiveInt(key string) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// preflightLoopback is the cheap pre-bind check: it rejects the obvious
// network-exposing mistakes (empty host = all interfaces, a non-loopback
// IP literal) with a clear error before any socket is opened. A
// hostname such as "localhost" passes preflight and is validated for
// real by verifyBoundLoopback AFTER the bind — its resolution cannot be
// trusted at this stage.
func preflightLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		return errors.New("pprof addr must specify a loopback host (e.g. 127.0.0.1:6060), refusing to bind all interfaces")
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
		return errors.New("pprof addr must bind a loopback address, got: " + host)
	}
	return nil
}

// verifyBoundLoopback inspects the listener's resolved local address and
// returns an error unless it is a loopback IP. This is the real guard:
// it catches a "localhost" (or any hostname) that the resolver mapped to
// a non-loopback interface, which preflightLoopback cannot detect.
func verifyBoundLoopback(ln net.Listener) error {
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return errors.New("pprof listener bound to a non-TCP address: " + ln.Addr().String())
	}
	if !tcpAddr.IP.IsLoopback() {
		return errors.New("pprof listener bound to a non-loopback address: " + tcpAddr.IP.String())
	}
	return nil
}
