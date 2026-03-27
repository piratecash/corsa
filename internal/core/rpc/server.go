package rpc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"corsa/internal/core/config"
	"corsa/internal/core/protocol"
	"github.com/gofiber/fiber/v3"
)

// Server is the HTTP layer for external RPC access.
// It wraps a CommandTable and exposes it via Fiber HTTP endpoints.
// Desktop UI should call CommandTable directly, not through Server.
type Server struct {
	app   *fiber.App
	cfg   config.RPC
	table *CommandTable
	node  NodeProvider
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
		AppName: "CORSA RPC",
	})

	server := &Server{
		app:   app,
		cfg:   cfg,
		table: table,
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

	// Raw frame passthrough: preserves all wire fields by forwarding
	// directly to HandleLocalFrame, bypassing CommandTable handlers.
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
	req.Command = strings.ToLower(req.Command)

	resp := s.table.Execute(CommandRequest{Name: req.Command, Args: req.Args})
	if resp.Error != nil {
		return ErrorResponse(c, resp.ErrorKind.HTTPStatus(), resp.Error.Error())
	}

	c.Set("Content-Type", "application/json")
	return c.Status(fiber.StatusOK).Send(resp.Data)
}

// handleFrame accepts a raw JSON protocol frame and dispatches it through
// CommandTable, falling back to HandleLocalFrame for unregistered frame types.
//
// CommandTable dispatch gives diagnostic-enriched responses for ping/get_peers
// (via DiagnosticProvider overrides) and chatlog support for fetch_chatlog,
// fetch_chatlog_previews, fetch_conversations (via ChatlogProvider). This
// matches the desktop console's ExecuteConsoleCommand behavior where these
// frame types get special handling instead of going directly to HandleLocalFrame.
//
// Unregistered frame types are forwarded to HandleLocalFrame, preserving all
// caller-supplied wire fields for custom/unknown frame types.
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
		// Unregistered frame type — forward raw frame to node.
		// This preserves all caller-supplied wire fields.
		frame, parseErr := protocol.ParseFrameLine(string(body))
		if parseErr != nil {
			return ErrorResponse(c, fiber.StatusBadRequest, fmt.Sprintf("invalid frame JSON: %v", parseErr))
		}
		reply := s.node.HandleLocalFrame(frame)
		data, marshalErr := json.Marshal(reply)
		if marshalErr != nil {
			return ErrorResponse(c, fiber.StatusInternalServerError, fmt.Sprintf("marshal response: %v", marshalErr))
		}
		c.Set("Content-Type", "application/json")
		return c.Status(fiber.StatusOK).Send(data)
	}

	if resp.Error != nil {
		return ErrorResponse(c, resp.ErrorKind.HTTPStatus(), resp.Error.Error())
	}

	c.Set("Content-Type", "application/json")
	return c.Status(fiber.StatusOK).Send(resp.Data)
}

// registerLegacyRoutes maps old per-endpoint paths to CommandTable dispatch.
// This keeps corsa-cli and external tools working without changes.
func (s *Server) registerLegacyRoutes(rpc fiber.Router) {
	// System
	rpc.Post("/system/help", s.legacyHandler("help"))
	rpc.Post("/system/ping", s.legacyHandler("ping"))
	rpc.Post("/system/hello", s.legacyHandler("hello"))
	rpc.Post("/system/version", s.legacyHandler("version"))

	// Network
	rpc.Post("/network/peers", s.legacyHandler("get_peers"))
	rpc.Post("/network/health", s.legacyHandler("fetch_peer_health"))
	rpc.Post("/network/add_peer", s.legacyArgHandler("add_peer"))

	// Identity
	rpc.Post("/identity/identities", s.legacyHandler("fetch_identities"))
	rpc.Post("/identity/contacts", s.legacyHandler("fetch_contacts"))
	rpc.Post("/identity/trusted_contacts", s.legacyHandler("fetch_trusted_contacts"))

	// Message
	rpc.Post("/message/list", s.legacyArgHandler("fetch_messages"))
	rpc.Post("/message/ids", s.legacyArgHandler("fetch_message_ids"))
	rpc.Post("/message/get", s.legacyArgHandler("fetch_message"))
	rpc.Post("/message/inbox", s.legacyArgHandler("fetch_inbox"))
	rpc.Post("/message/pending", s.legacyArgHandler("fetch_pending_messages"))
	rpc.Post("/message/receipts", s.legacyArgHandler("fetch_delivery_receipts"))
	rpc.Post("/message/dm_headers", s.legacyHandler("fetch_dm_headers"))
	rpc.Post("/message/send_dm", s.legacyArgHandler("send_dm"))

	// Chatlog
	rpc.Post("/chatlog/entries", s.legacyArgHandler("fetch_chatlog"))
	rpc.Post("/chatlog/previews", s.legacyHandler("fetch_chatlog_previews"))
	rpc.Post("/chatlog/conversations", s.legacyHandler("fetch_conversations"))

	// Notice
	rpc.Post("/notice/list", s.legacyHandler("fetch_notices"))
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
func (s *Server) authMiddleware() fiber.Handler {
	username := s.cfg.Username
	password := s.cfg.Password

	unauthorizedResponse := func(c fiber.Ctx, msg string) error {
		c.Set("WWW-Authenticate", `Basic realm="corsa-rpc"`)
		return ErrorResponse(c, fiber.StatusUnauthorized, msg)
	}

	return func(c fiber.Ctx) error {
		auth := c.Get("Authorization", "")
		if auth == "" {
			return unauthorizedResponse(c, "missing authorization header")
		}

		if !strings.HasPrefix(auth, "Basic ") {
			return unauthorizedResponse(c, "invalid authorization header")
		}

		encoded := strings.TrimPrefix(auth, "Basic ")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return unauthorizedResponse(c, "invalid base64 encoding")
		}

		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 {
			return unauthorizedResponse(c, "invalid credentials format")
		}

		if parts[0] != username || parts[1] != password {
			return unauthorizedResponse(c, "invalid credentials")
		}

		return c.Next()
	}
}
