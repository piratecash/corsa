package rpc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gofiber/fiber/v3"
	"github.com/piratecash/corsa/internal/core/config"
)

// rpcMaxBodyBytes is the maximum HTTP request body size accepted by the RPC
// server. Without an explicit limit, Fiber defaults to 4 MiB which is
// reasonable, but setting it explicitly ensures the value is intentional
// and auditable. 1 MiB is sufficient for all RPC commands — the largest
// payload is send_dm with a sealed DM body (~87 KiB base64).
const rpcMaxBodyBytes = 1 * 1024 * 1024

// authMaxAttempts is the maximum number of failed authentication attempts
// allowed from a single IP within authWindowDuration before temporary lockout.
const authMaxAttempts = 10

// authWindowDuration is the sliding window for tracking auth failures.
const authWindowDuration = 5 * time.Minute

// authLockoutDuration is how long an IP is locked out after exceeding
// authMaxAttempts failed authentication attempts.
const authLockoutDuration = 15 * time.Minute

// Server is the HTTP layer for external RPC access.
// It wraps a CommandTable and exposes it via Fiber HTTP endpoints.
// Desktop UI should call CommandTable directly, not through Server.
type Server struct {
	app         *fiber.App
	cfg         config.RPC
	table       *CommandTable
	node        NodeProvider
	authLimiter *authRateLimiter
}

// NewServer creates a new RPC HTTP server backed by the given CommandTable.
// The optional NodeProvider enables the /rpc/v1/frame endpoint for raw JSON
// frame passthrough. Pass nil to disable that endpoint (tests may omit it).
// Returns an error if configuration is invalid (e.g. partial auth credentials).
func NewServer(cfg config.RPC, table *CommandTable, node ...NodeProvider) (*Server, error) {
	if err := cfg.ValidateAuth(); err != nil {
		return nil, fmt.Errorf("rpc config invalid: %w", err)
	}

	app := fiber.New(fiber.Config{
		AppName:   "CORSA RPC",
		BodyLimit: rpcMaxBodyBytes,
	})

	server := &Server{
		app:         app,
		cfg:         cfg,
		table:       table,
		authLimiter: newAuthRateLimiter(),
	}
	if len(node) > 0 {
		server.node = node[0]
	}

	if cfg.AuthEnabled() {
		app.Use(server.authMiddleware())
	}

	// Universal dispatch: all commands go through CommandTable.
	// POST /rpc/v1/exec {"command": "ping", "args": {...}}
	rpcGroup := app.Group("/rpc/v1")
	rpcGroup.Post("/exec", server.handleExec)

	// Raw frame dispatch: parses "type" from JSON, normalizes wire field
	// names, then dispatches through CommandTable. Unregistered frame types
	// are rejected with 400 — no bypass to HandleLocalFrame.
	if server.node != nil {
		rpcGroup.Post("/frame", server.handleFrame)
	}

	// Legacy route-based endpoints for backward compatibility with corsa-cli.
	server.registerLegacyRoutes(rpcGroup)

	return server, nil
}

// Start starts the RPC server listening on the configured address (blocking).
func (s *Server) Start() error {
	listenAddr := s.cfg.ListenAddress()
	return s.app.Listen(listenAddr, fiber.ListenConfig{
		DisableStartupMessage: true,
	})
}

// startAsyncTimeout is the maximum time StartAsync waits for the server to
// bind before returning an error. Prevents indefinite blocking if Fiber
// stalls before triggering OnListen or returning a Listen error.
const startAsyncTimeout = 10 * time.Second

// StartAsync launches the RPC server in a background goroutine.
// Returns an error if the server fails to bind within startAsyncTimeout.
// Logs the actual listen address on success.
func (s *Server) StartAsync() error {
	ready := make(chan struct{}, 1)
	s.app.Hooks().OnListen(func(ld fiber.ListenData) error {
		log.Info().
			Str("listen", fmt.Sprintf("%s:%s", ld.Host, ld.Port)).
			Bool("auth", s.cfg.AuthEnabled()).
			Msg("rpc server started")
		select {
		case ready <- struct{}{}:
		default:
		}
		return nil
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start()
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("rpc server failed to start: %w", err)
	case <-ready:
		return nil
	case <-time.After(startAsyncTimeout):
		return fmt.Errorf("rpc server failed to start: bind timeout after %s", startAsyncTimeout)
	}
}

// Shutdown gracefully shuts down the RPC server.
func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}

// handleExec is the universal command dispatcher.
// POST /rpc/v1/exec {"command": "ping", "args": {...}}
func (s *Server) handleExec(c fiber.Ctx) error {
	var req struct {
		Command string                 `json:"command"`
		Args    map[string]interface{} `json:"args"`
	}
	if err := c.Bind().JSON(&req); err != nil {
		return ErrorResponse(c, fiber.StatusBadRequest, "invalid request body")
	}
	req.Command = strings.TrimSpace(req.Command)
	if req.Command == "" {
		return ErrorResponse(c, fiber.StatusBadRequest, "command is required")
	}

	// CommandTable.Execute resolves camelCase, snake_case, and case-insensitive
	// input transparently — no need to lowercase here.
	resp := s.table.Execute(CommandRequest{Name: req.Command, Args: req.Args})
	if resp.Error != nil {
		return ErrorResponse(c, resp.ErrorKind.HTTPStatus(), resp.Error.Error())
	}

	c.Set("Content-Type", "application/json")
	return c.Status(fiber.StatusOK).Send(resp.Data)
}

// handleFrame accepts a raw JSON protocol frame and dispatches it through
// CommandTable. Only CommandTable-registered frame types are accepted.
//
// CommandTable dispatch provides chatlog support for fetch_chatlog,
// fetch_chatlog_previews, fetch_conversations (via ChatlogProvider) and a
// transport-level hello override (via DiagnosticProvider) that identifies
// the desktop client. Raw commands (ping, getPeers) go directly through
// HandleLocalFrame without enrichment.
//
// Unregistered frame types are REJECTED (400 Bad Request). Previously they
// were forwarded to HandleLocalFrame, but this allowed HTTP clients to inject
// network-level frames (relay_message, push_message, subscribe_inbox) that
// bypass P2P authentication. Only CommandTable-registered types have proper
// validation and authorization checks for the RPC context.
//
// POST /rpc/v1/frame {"type":"hello","client":"desktop","client_version":"2.0"}
func (s *Server) handleFrame(c fiber.Ctx) error {
	body := c.Body()
	if len(body) == 0 {
		return ErrorResponse(c, fiber.StatusBadRequest, "empty request body")
	}

	// Parse raw JSON as a CommandRequest: extracts "type" as command name,
	// normalizes wire field names to RPC arg names (e.g. "peers" → "address").
	req, err := parseJSONFrame(string(body))
	if err != nil {
		return ErrorResponse(c, fiber.StatusBadRequest, err.Error())
	}

	resp := s.table.Execute(req)

	if resp.ErrorKind == ErrNotFound {
		// Unregistered frame type — reject. Previously, unknown frame types
		// were forwarded to HandleLocalFrame, which processes them as if
		// they came from an authenticated P2P peer. This allowed HTTP
		// clients to inject relay_message, push_message, subscribe_inbox
		// and other network-level frames, bypassing P2P authentication.
		//
		// Only CommandTable-registered frame types are safe for RPC dispatch
		// because they have explicit validation and authorization checks.
		return ErrorResponse(c, fiber.StatusBadRequest, fmt.Sprintf("unknown frame type: %s", req.Name))
	}

	if resp.Error != nil {
		return ErrorResponse(c, resp.ErrorKind.HTTPStatus(), resp.Error.Error())
	}

	c.Set("Content-Type", "application/json")
	return c.Status(fiber.StatusOK).Send(resp.Data)
}

// registerLegacyRoutes maps old per-endpoint paths to CommandTable dispatch.
// This keeps corsa-cli and external tools working without changes.
// Commands are dispatched by their canonical camelCase names.
func (s *Server) registerLegacyRoutes(rpc fiber.Router) {
	// System
	rpc.Post("/system/help", s.legacyHandler("help"))
	rpc.Post("/system/ping", s.legacyHandler("ping"))
	rpc.Post("/system/hello", s.legacyHandler("hello"))
	rpc.Post("/system/version", s.legacyHandler("version"))

	// Network
	rpc.Post("/network/peers", s.legacyHandler("getPeers"))
	rpc.Post("/network/health", s.legacyHandler("fetchPeerHealth"))
	rpc.Post("/network/stats", s.legacyHandler("fetchNetworkStats"))
	rpc.Post("/network/add_peer", s.legacyArgHandler("addPeer"))

	// Metrics
	rpc.Post("/metrics/traffic_history", s.legacyHandler("fetchTrafficHistory"))

	// Identity
	rpc.Post("/identity/identities", s.legacyHandler("fetchIdentities"))
	rpc.Post("/identity/contacts", s.legacyHandler("fetchContacts"))
	rpc.Post("/identity/trusted_contacts", s.legacyHandler("fetchTrustedContacts"))

	// Message
	rpc.Post("/message/list", s.legacyArgHandler("fetchMessages"))
	rpc.Post("/message/ids", s.legacyArgHandler("fetchMessageIds"))
	rpc.Post("/message/get", s.legacyArgHandler("fetchMessage"))
	rpc.Post("/message/inbox", s.legacyArgHandler("fetchInbox"))
	rpc.Post("/message/pending", s.legacyArgHandler("fetchPendingMessages"))
	rpc.Post("/message/receipts", s.legacyArgHandler("fetchDeliveryReceipts"))
	rpc.Post("/message/dm_headers", s.legacyHandler("fetchDmHeaders"))
	rpc.Post("/message/send_dm", s.legacyArgHandler("sendDm"))

	// Chatlog
	rpc.Post("/chatlog/entries", s.legacyArgHandler("fetchChatlog"))
	rpc.Post("/chatlog/previews", s.legacyHandler("fetchChatlogPreviews"))
	rpc.Post("/chatlog/conversations", s.legacyHandler("fetchConversations"))

	// Notice
	rpc.Post("/notice/list", s.legacyHandler("fetchNotices"))

	// Mesh
	rpc.Post("/mesh/relay_status", s.legacyHandler("fetchRelayStatus"))

	// File Transfer
	rpc.Post("/file/send_file_announce", s.legacyArgHandler("sendFileAnnounce"))
	rpc.Post("/file/transfers", s.legacyHandler("fetchFileTransfers"))
	rpc.Post("/file/mapping", s.legacyHandler("fetchFileMapping"))
	rpc.Post("/file/retry_chunk", s.legacyArgHandler("retryFileChunk"))
	rpc.Post("/file/start_download", s.legacyArgHandler("startFileDownload"))
	rpc.Post("/file/cancel_download", s.legacyArgHandler("cancelFileDownload"))
}

// legacyHandler creates a Fiber handler for a no-args command.
// Mode-gated commands return 503 via ErrUnavailable from CommandTable.Execute().
func (s *Server) legacyHandler(command string) fiber.Handler {
	return func(c fiber.Ctx) error {
		resp := s.table.Execute(CommandRequest{Name: command})
		if resp.Error != nil {
			return ErrorResponse(c, resp.ErrorKind.HTTPStatus(), resp.Error.Error())
		}
		c.Set("Content-Type", "application/json")
		return c.Status(fiber.StatusOK).Send(resp.Data)
	}
}

// legacyArgHandler creates a Fiber handler that reads JSON body as command args.
// Mode-gated commands return 503 via ErrUnavailable from CommandTable.Execute().
func (s *Server) legacyArgHandler(command string) fiber.Handler {
	return func(c fiber.Ctx) error {
		var args map[string]interface{}
		body := c.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &args); err != nil {
				return ErrorResponse(c, fiber.StatusBadRequest, "invalid request body")
			}
		}
		resp := s.table.Execute(CommandRequest{Name: command, Args: args})
		if resp.Error != nil {
			return ErrorResponse(c, resp.ErrorKind.HTTPStatus(), resp.Error.Error())
		}
		c.Set("Content-Type", "application/json")
		return c.Status(fiber.StatusOK).Send(resp.Data)
	}
}

// authMiddleware returns a Fiber handler that validates HTTP Basic authentication.
// Per RFC 7235 §3.1, every 401 response includes a WWW-Authenticate header
// so clients know which scheme to use.
//
// Includes per-IP rate limiting of failed authentication attempts to prevent
// brute-force credential guessing. After authMaxAttempts failures within
// authWindowDuration, the IP is locked out for authLockoutDuration.
func (s *Server) authMiddleware() fiber.Handler {
	username := s.cfg.Username
	password := s.cfg.Password

	unauthorizedResponse := func(c fiber.Ctx, msg string) error {
		c.Set("WWW-Authenticate", `Basic realm="corsa-rpc"`)
		return ErrorResponse(c, fiber.StatusUnauthorized, msg)
	}

	return func(c fiber.Ctx) error {
		ip := c.IP()

		// Check if this IP is currently locked out due to too many failures.
		if s.authLimiter.isLockedOut(ip) {
			return ErrorResponse(c, fiber.StatusTooManyRequests, "too many authentication attempts")
		}

		auth := c.Get("Authorization", "")
		if auth == "" {
			s.authLimiter.recordFailure(ip)
			return unauthorizedResponse(c, "missing authorization header")
		}

		if !strings.HasPrefix(auth, "Basic ") {
			s.authLimiter.recordFailure(ip)
			return unauthorizedResponse(c, "invalid authorization header")
		}

		encoded := strings.TrimPrefix(auth, "Basic ")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			s.authLimiter.recordFailure(ip)
			return unauthorizedResponse(c, "invalid base64 encoding")
		}

		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 {
			s.authLimiter.recordFailure(ip)
			return unauthorizedResponse(c, "invalid credentials format")
		}

		if parts[0] != username || parts[1] != password {
			s.authLimiter.recordFailure(ip)
			return unauthorizedResponse(c, "invalid credentials")
		}

		// Successful auth — reset failure count for this IP.
		s.authLimiter.resetFailures(ip)
		return c.Next()
	}
}

// ---------------------------------------------------------------------------
// Auth rate limiter — prevents brute-force credential guessing
// ---------------------------------------------------------------------------

// authRateLimiter tracks failed authentication attempts per IP and enforces
// temporary lockouts after too many failures.
type authRateLimiter struct {
	mu       sync.Mutex
	failures map[string]*authFailureRecord
}

type authFailureRecord struct {
	attempts    []time.Time
	lockedUntil time.Time
}

func newAuthRateLimiter() *authRateLimiter {
	return &authRateLimiter{
		failures: make(map[string]*authFailureRecord),
	}
}

// isLockedOut returns true if the IP is currently locked out.
func (al *authRateLimiter) isLockedOut(ip string) bool {
	al.mu.Lock()
	defer al.mu.Unlock()
	rec, ok := al.failures[ip]
	if !ok {
		return false
	}
	if time.Now().Before(rec.lockedUntil) {
		return true
	}
	return false
}

// recordFailure records a failed authentication attempt from the given IP.
// If the number of recent failures exceeds the threshold, locks out the IP.
func (al *authRateLimiter) recordFailure(ip string) {
	al.mu.Lock()
	defer al.mu.Unlock()

	now := time.Now()
	rec, ok := al.failures[ip]
	if !ok {
		rec = &authFailureRecord{}
		al.failures[ip] = rec
	}

	// Prune attempts outside the window.
	cutoff := now.Add(-authWindowDuration)
	recent := rec.attempts[:0]
	for _, t := range rec.attempts {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	recent = append(recent, now)
	rec.attempts = recent

	if len(rec.attempts) >= authMaxAttempts {
		rec.lockedUntil = now.Add(authLockoutDuration)
		log.Warn().Str("ip", ip).Int("attempts", len(rec.attempts)).Msg("auth rate limit lockout")
	}
}

// resetFailures clears the failure record for an IP after successful auth.
func (al *authRateLimiter) resetFailures(ip string) {
	al.mu.Lock()
	defer al.mu.Unlock()
	delete(al.failures, ip)
}

// cleanup removes stale entries to prevent unbounded map growth.
func (al *authRateLimiter) cleanup() {
	al.mu.Lock()
	defer al.mu.Unlock()
	now := time.Now()
	for ip, rec := range al.failures {
		if now.After(rec.lockedUntil) && len(rec.attempts) == 0 {
			delete(al.failures, ip)
			continue
		}
		// Prune old attempts.
		cutoff := now.Add(-authWindowDuration)
		recent := rec.attempts[:0]
		for _, t := range rec.attempts {
			if t.After(cutoff) {
				recent = append(recent, t)
			}
		}
		rec.attempts = recent
		if len(rec.attempts) == 0 && now.After(rec.lockedUntil) {
			delete(al.failures, ip)
		}
	}
}
