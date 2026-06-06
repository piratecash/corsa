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
