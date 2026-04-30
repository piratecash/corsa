package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/service"
)

// CommandRequest holds the parsed input for a command execution.
type CommandRequest struct {
	Name string
	Args map[string]interface{}

	// Ctx carries cancellation for long-running commands. Handlers that
	// perform I/O should check Ctx.Done() and return early when the
	// caller abandons the request. Nil means no cancellation — handlers
	// must treat a nil Ctx the same as context.Background().
	Ctx context.Context
}

// ErrorKind categorizes command errors so the HTTP layer can map them
// to correct status codes without inspecting error messages.
type ErrorKind int

const (
	// ErrValidation means the caller sent bad input (missing/invalid args).
	ErrValidation ErrorKind = iota

	// ErrInternal means the command failed due to a provider or system error.
	ErrInternal

	// ErrNotFound means the requested command does not exist in the table.
	ErrNotFound

	// ErrUnavailable means the command exists but is not available in this
	// operating mode (e.g. chatlog commands on a standalone node).
	ErrUnavailable
)

// HTTPStatus returns the HTTP status code corresponding to this error kind.
// Uses net/http constants — CommandTable is transport-agnostic, no Fiber dependency.
func (k ErrorKind) HTTPStatus() int {
	switch k {
	case ErrValidation:
		return http.StatusBadRequest
	case ErrNotFound:
		return http.StatusNotFound
	case ErrUnavailable:
		return http.StatusServiceUnavailable
	case ErrInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

// CommandResponse holds the result of a command execution.
type CommandResponse struct {
	// Data contains the response payload.
	// For frame-based commands this is a serialized protocol.Frame.
	// For raw JSON commands (chatlog) this is the raw JSON string.
	Data json.RawMessage

	// Error is set when the command fails.
	Error error

	// ErrorKind categorizes the error for HTTP status mapping.
	// Only meaningful when Error is non-nil.
	ErrorKind ErrorKind
}

// CommandHandler is a function that executes a single RPC command.
type CommandHandler func(req CommandRequest) CommandResponse

// CommandTable is the single source of truth for command registration and execution.
// Both the Fiber HTTP layer and the desktop UI console call into this table.
// No HTTP, no network — pure in-process function dispatch.
type CommandTable struct {
	mu          sync.RWMutex
	handlers    map[string]CommandHandler
	metadata    map[string]CommandInfo
	unavailable map[string]bool   // mode-gated commands registered as unavailable
	aliases     map[string]string // alias → canonical name (deprecated snake_case → camelCase)
}

// NewCommandTable creates an empty command table.
func NewCommandTable() *CommandTable {
	return &CommandTable{
		handlers:    make(map[string]CommandHandler),
		metadata:    make(map[string]CommandInfo),
		unavailable: make(map[string]bool),
		aliases:     make(map[string]string),
	}
}

// Register adds a command handler with metadata.
// Overwrites if the command name already exists.
func (t *CommandTable) Register(info CommandInfo, handler CommandHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[info.Name] = handler
	t.metadata[info.Name] = info
	delete(t.unavailable, info.Name)
}

// RegisterAlias maps a deprecated command name to its canonical replacement.
// Execute() resolves aliases transparently — callers using the old name get
// the same handler as the new name. Aliases do NOT appear in Commands() or
// AllNames() to keep help output clean. Has() returns true for aliases.
//
// Intended for the snake_case → camelCase migration: old names are kept as
// aliases for 2 releases, then removed.
func (t *CommandTable) RegisterAlias(oldName, canonicalName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.aliases[oldName] = canonicalName
}

// RegisterUnavailable registers a command that exists but is not available in this
// operating mode. Execute() returns ErrUnavailable/503 instead of ErrNotFound/404.
// The command does NOT appear in Commands() (help output, autocomplete).
func (t *CommandTable) RegisterUnavailable(info CommandInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[info.Name] = func(req CommandRequest) CommandResponse {
		return CommandResponse{
			Error:     fmt.Errorf("%s not available in this mode", info.Name),
			ErrorKind: ErrUnavailable,
		}
	}
	t.metadata[info.Name] = info
	t.unavailable[info.Name] = true
}

// resolveHandler looks up a handler by exact name, then by alias, then by
// case-insensitive match against registered names. Returns the handler and
// the canonical name, or nil if not found.
//
// Caller must hold at least t.mu.RLock().
func (t *CommandTable) resolveHandler(name string) (CommandHandler, string, bool) {
	// 1. Exact match (fast path for correctly-cased callers).
	if h, ok := t.handlers[name]; ok {
		return h, name, true
	}
	// 2. Alias (snake_case backward compat).
	if canonical, ok := t.aliases[name]; ok {
		if h, ok := t.handlers[canonical]; ok {
			return h, canonical, true
		}
	}
	// 3. Case-insensitive fallback — handles lowercased console input matching
	//    camelCase command names (e.g. "getpeers" → "getPeers").
	lower := strings.ToLower(name)
	for n, h := range t.handlers {
		if strings.ToLower(n) == lower {
			return h, n, true
		}
	}
	// 4. Case-insensitive alias lookup.
	for old, canonical := range t.aliases {
		if strings.ToLower(old) == lower {
			if h, ok := t.handlers[canonical]; ok {
				return h, canonical, true
			}
		}
	}
	return nil, "", false
}

// Execute runs a command by name and returns the response.
// Aliases and case-insensitive names are resolved transparently.
// If req.Ctx is set and already cancelled, Execute returns immediately
// without dispatching the handler — this prevents orphaned goroutines
// from mutating node state after the caller has given up.
func (t *CommandTable) Execute(req CommandRequest) CommandResponse {
	if req.Ctx != nil {
		if err := req.Ctx.Err(); err != nil {
			return CommandResponse{
				Error:     fmt.Errorf("command cancelled: %w", err),
				ErrorKind: ErrInternal,
			}
		}
	}

	t.mu.RLock()
	handler, canonical, exists := t.resolveHandler(req.Name)
	t.mu.RUnlock()

	if !exists {
		return CommandResponse{
			Error:     fmt.Errorf("unknown command: %s", req.Name),
			ErrorKind: ErrNotFound,
		}
	}
	req.Name = canonical
	return handler(req)
}

// Commands returns metadata for all available commands, sorted by name.
// Unavailable (mode-gated) commands are excluded from the list.
// Deterministic order is important for help output and UI autocomplete.
func (t *CommandTable) Commands() []CommandInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	cmds := make([]CommandInfo, 0, len(t.metadata))
	for _, info := range t.metadata {
		if !t.unavailable[info.Name] {
			cmds = append(cmds, info)
		}
	}
	sort.Slice(cmds, func(i, j int) bool {
		return cmds[i].Name < cmds[j].Name
	})
	return cmds
}

// Has returns true if the command is registered (directly, via alias, or
// case-insensitive match).
func (t *CommandTable) Has(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, _, exists := t.resolveHandler(name)
	return exists
}

// AllNames returns names of all registered commands, including unavailable ones.
// Sorted alphabetically for deterministic output.
func (t *CommandTable) AllNames() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	names := make([]string, 0, len(t.handlers))
	for name := range t.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// RegisterAllCommands registers every command group into the table.
// This is the single registration point — used by the application bootstrap
// and by tests that need a fully populated CommandTable.
// Pass nil for chatlog, dmRouter, or metricsProvider to simulate standalone
// node mode — those commands are registered as unavailable (503, hidden from help).
//
// If node additionally implements RoutingProvider or ConnectionDiagnosticProvider,
// the corresponding command groups are registered; otherwise they become 503.
//
// All commands use camelCase names. Snake_case aliases are registered for
// backward compatibility and will be removed after 2 releases.
func RegisterAllCommands(t *CommandTable, node NodeProvider, chatlog ChatlogProvider, dmRouter DMRouterProvider, metricsProvider MetricsProvider) {
	RegisterSystemCommands(t, node)
	RegisterNetworkCommands(t, node)
	RegisterMeshCommands(t, node)
	RegisterIdentityCommands(t, node)
	RegisterMessageCommands(t, node, dmRouter, chatlog)
	RegisterFileCommands(t, node, dmRouter)
	RegisterChatlogCommands(t, chatlog)
	RegisterNoticeCommands(t, node)
	RegisterMetricsCommands(t, metricsProvider)

	// Node optionally implements RoutingProvider and ConnectionDiagnosticProvider.
	// Extract via type assertion — nil means the feature is unavailable and
	// the corresponding commands are registered as 503.
	var rp RoutingProvider
	if r, ok := node.(RoutingProvider); ok {
		rp = r
	}
	RegisterRoutingCommands(t, rp)

	var cd ConnectionDiagnosticProvider
	if c, ok := node.(ConnectionDiagnosticProvider); ok {
		cd = c
	}
	RegisterConnectionCommands(t, cd)

	var cp CaptureProvider
	if c, ok := node.(CaptureProvider); ok {
		cp = c
	}
	RegisterCaptureCommands(t, cp)

	registerSnakeCaseAliases(t)
}

// registerSnakeCaseAliases maps deprecated snake_case command names to their
// canonical camelCase replacements. Kept for 2 releases to give clients time
// to migrate. Remove after v0.18.
func registerSnakeCaseAliases(t *CommandTable) {
	aliases := map[string]string{
		"get_peers":                      "getPeers",
		"fetch_peer_health":              "fetchPeerHealth",
		"fetch_network_stats":            "fetchNetworkStats",
		"add_peer":                       "addPeer",
		"fetch_reachable_ids":            "fetchReachableIds",
		"fetch_aggregate_status":         "fetchAggregateStatus",
		"fetch_relay_status":             "fetchRelayStatus",
		"fetch_identities":               "fetchIdentities",
		"fetch_contacts":                 "fetchContacts",
		"fetch_trusted_contacts":         "fetchTrustedContacts",
		"delete_trusted_contact":         "deleteTrustedContact",
		"import_contacts":                "importContacts",
		"fetch_messages":                 "fetchMessages",
		"fetch_message_ids":              "fetchMessageIds",
		"fetch_message":                  "fetchMessage",
		"fetch_inbox":                    "fetchInbox",
		"fetch_pending_messages":         "fetchPendingMessages",
		"fetch_delivery_receipts":        "fetchDeliveryReceipts",
		"fetch_dm_headers":               "fetchDmHeaders",
		"send_dm":                        "sendDm",
		"delete_dm":                      "deleteDm",
		"send_message":                   "sendMessage",
		"import_message":                 "importMessage",
		"send_delivery_receipt":          "sendDeliveryReceipt",
		"send_file_announce":             "sendFileAnnounce",
		"fetch_file_transfers":           "fetchFileTransfers",
		"fetch_all_file_transfers":       "fetchAllFileTransfers",
		"fetch_file_mapping":             "fetchFileMapping",
		"retry_file_chunk":               "retryFileChunk",
		"start_file_download":            "startFileDownload",
		"cancel_file_download":           "cancelFileDownload",
		"restart_file_download":          "restartFileDownload",
		"explain_file_route":             "explainFileRoute",
		"fetch_chatlog":                  "fetchChatlog",
		"fetch_chatlog_previews":         "fetchChatlogPreviews",
		"fetch_conversations":            "fetchConversations",
		"fetch_notices":                  "fetchNotices",
		"publish_notice":                 "publishNotice",
		"fetch_traffic_history":          "fetchTrafficHistory",
		"fetch_route_table":              "fetchRouteTable",
		"fetch_route_summary":            "fetchRouteSummary",
		"fetch_route_lookup":             "fetchRouteLookup",
		"get_active_peers":               "getActivePeers",
		"get_active_connections":         "getActiveConnections",
		"list_peers":                     "listPeers",
		"list_banned":                    "listBanned",
		"record_peer_traffic_by_conn_id": "recordPeerTrafficByConnID",
		"record_peer_traffic_by_ip":      "recordPeerTrafficByIP",
		"record_all_peer_traffic":        "recordAllPeerTraffic",
		"stop_peer_traffic_recording":    "stopPeerTrafficRecording",
		// PIP-0001 integration: alternative spellings for callers that
		// follow the older or compact convention. The canonical
		// camelCase form is getNodeStatus.
		//
		// "nodestatus" must be a real alias (not just a case-insensitive
		// fallback): strings.ToLower("getNodeStatus") yields
		// "getnodestatus", so a caller posting {"command":"nodestatus"}
		// would otherwise miss every resolveHandler branch and get a
		// 404. The PIP doc and the snake-case-alias test both promise
		// this spelling works, so an explicit alias is the only fix.
		"nodestatus":      "getNodeStatus",
		"node_status":     "getNodeStatus",
		"get_node_status": "getNodeStatus",
	}
	for old, canonical := range aliases {
		t.RegisterAlias(old, canonical)
	}
}

// --- Helper constructors for building responses ---

// frameResponse serializes a protocol.Frame into a CommandResponse.
func frameResponse(frame protocol.Frame) CommandResponse {
	data, err := json.Marshal(frame)
	if err != nil {
		return internalError(fmt.Errorf("marshal frame: %w", err))
	}
	return CommandResponse{Data: data}
}

// jsonResponse serializes any value into a CommandResponse.
func jsonResponse(v interface{}) CommandResponse {
	data, err := json.Marshal(v)
	if err != nil {
		return internalError(fmt.Errorf("marshal response: %w", err))
	}
	return CommandResponse{Data: data}
}

// rawJSONResponse wraps a pre-serialized JSON string.
func rawJSONResponse(raw string) CommandResponse {
	return CommandResponse{Data: json.RawMessage(raw)}
}

// validationError creates a CommandResponse for caller-side input errors (400).
func validationError(err error) CommandResponse {
	return CommandResponse{Error: err, ErrorKind: ErrValidation}
}

// internalError creates a CommandResponse for provider/system failures (500).
func internalError(err error) CommandResponse {
	return CommandResponse{Error: err, ErrorKind: ErrInternal}
}

// unavailableError creates a CommandResponse for transient
// service-state rejections (503): the command exists and the
// input is well-formed, but the call cannot be served right now
// (e.g. an in-flight conversation_delete is blocking sends to the
// peer). Callers can retry once the underlying state clears.
func unavailableError(err error) CommandResponse {
	return CommandResponse{Error: err, ErrorKind: ErrUnavailable}
}

// ctxDone checks whether the command's context has been cancelled.
// Returns a cancellation CommandResponse and true when the caller should
// short-circuit, or a zero CommandResponse and false when the handler may
// proceed. This is called inside handler closures at the boundary before
// (and between) expensive operations so that abandoned console commands
// stop as early as possible without requiring deep context threading
// through HandleLocalFrame and its callees.
func ctxDone(req CommandRequest) (CommandResponse, bool) {
	if req.Ctx == nil {
		return CommandResponse{}, false
	}
	if err := req.Ctx.Err(); err != nil {
		return CommandResponse{
			Error:     fmt.Errorf("command cancelled: %w", err),
			ErrorKind: ErrInternal,
		}, true
	}
	return CommandResponse{}, false
}

// numericArg extracts a numeric argument that may arrive as float64 (JSON path)
// or as a string (key=value / positional path). Returns 0 and false when the
// key is absent or the value cannot be interpreted as a positive number.
func numericArg(args map[string]interface{}, key string) (int, bool) {
	v, exists := args[key]
	if !exists {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int(n), n > 0
	case string:
		i, err := strconv.Atoi(n)
		return i, err == nil && i > 0
	default:
		return 0, false
	}
}

// frameFromArgs reconstructs a protocol.Frame from CommandRequest.Args.
// When raw JSON is pasted into the console, parseJSONFrame puts all fields
// into the args map. This helper round-trips through JSON to populate the
// typed Frame struct, preserving all wire fields without per-field mapping.
// The "type" field is set explicitly to ensure it matches the registered
// command name (args may contain a different-cased or absent "type").
func frameFromArgs(commandType string, args map[string]interface{}) (protocol.Frame, error) {
	if args == nil {
		return protocol.Frame{Type: commandType}, nil
	}
	data, err := json.Marshal(args)
	if err != nil {
		return protocol.Frame{}, fmt.Errorf("marshal args: %w", err)
	}
	var frame protocol.Frame
	if err := json.Unmarshal(data, &frame); err != nil {
		return protocol.Frame{}, fmt.Errorf("unmarshal frame: %w", err)
	}
	frame.Type = commandType
	return frame, nil
}

// --- Registration helpers for common patterns ---

// helpSchemaVersion is the version of the help response format.
// Bump when the structure of the help response changes (e.g. new fields
// in CommandInfo). This is NOT the protocol or client version — those
// are returned by the separate "version" command.
const helpSchemaVersion = "1.0"

// RegisterSystemCommands registers help, ping, hello, version, and getNodeStatus.
func RegisterSystemCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "help", Description: "List all available RPC commands", Category: "system"},
		func(req CommandRequest) CommandResponse {
			return jsonResponse(map[string]interface{}{
				"commands": t.Commands(),
				"version":  helpSchemaVersion,
			})
		},
	)

	t.Register(
		CommandInfo{Name: "ping", Description: "Send local ping and receive pong response", Category: "system"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "ping"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "hello", Description: "Send hello frame to identify with peers", Category: "system"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{
				Type:                   "hello",
				Version:                config.ProtocolVersion,
				MinimumProtocolVersion: config.MinimumProtocolVersion,
				Client:                 "rpc",
				ClientVersion:          node.ClientVersion(),
				ClientBuild:            config.ClientBuild,
			})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "version", Description: "Get client version and protocol version", Category: "system"},
		func(req CommandRequest) CommandResponse {
			return jsonResponse(map[string]interface{}{
				"client_version":   node.ClientVersion(),
				"protocol_version": config.ProtocolVersion,
				"node_address":     node.Address(),
			})
		},
	)

	// getNodeStatus is the PIP-0001 integration point for PirateCash
	// Core: a single authenticated RPC call that returns identity,
	// public-key material, protocol/version, peer count, and uptime.
	// Stages 1 and 2 of PIP-0001 use it as a liveness probe; stage 3
	// (v21 PoSe) will derive proof-of-service signatures from the
	// public_key reported here. See docs/rpc/system.md.
	//
	// Snake_case alias `node_status` is registered alongside the
	// canonical camelCase name; case-insensitive resolution
	// additionally accepts `nodestatus`, `nodeStatus`, and any other
	// case combination so PirateCash Core implementations can pick
	// whichever spelling matches their conventions.
	t.Register(
		CommandInfo{Name: "getNodeStatus", Description: "Get node identity, public keys, protocol/version, peer count, and uptime — PIP-0001 integration surface", Category: "system"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			return jsonResponse(node.NodeStatus())
		},
	)
}

// RegisterNetworkCommands registers getPeers, fetchPeerHealth, fetchNetworkStats,
// addPeer, fetchReachableIds, fetchAggregateStatus.
func RegisterNetworkCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "getPeers", Description: "Get list of connected peers", Category: "network"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "get_peers"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetchPeerHealth", Description: "Get peer health status", Category: "network"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetchNetworkStats", Description: "Get aggregated network traffic statistics per peer and total", Category: "network"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "addPeer", Description: "Add a new peer by address", Category: "network", Usage: "<address>"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			address, _ := req.Args["address"].(string)
			if strings.TrimSpace(address) == "" {
				return validationError(fmt.Errorf("address is required"))
			}
			reply := node.HandleLocalFrame(protocol.Frame{
				Type:  "add_peer",
				Peers: []string{address},
			})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetchReachableIds", Description: "Get identities reachable via mesh routing", Category: "network"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			return frameResponse(node.HandleLocalFrame(protocol.Frame{Type: "fetch_reachable_ids"}))
		},
	)

	t.Register(
		CommandInfo{Name: "fetchAggregateStatus", Description: "Get aggregate network health status (single source of truth for policy and UI)", Category: "network"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			return frameResponse(node.HandleLocalFrame(protocol.Frame{Type: "fetch_aggregate_status"}))
		},
	)
}

// RegisterConnectionCommands registers getActivePeers, getActiveConnections,
// listPeers, listBanned.
// When the ConnectionDiagnosticProvider is nil (CM/PP not wired), commands
// are registered as unavailable — hidden from help/autocomplete and returning
// 503 on execution.
func RegisterConnectionCommands(t *CommandTable, cd ConnectionDiagnosticProvider) {
	activePeersInfo := CommandInfo{Name: "getActivePeers", Description: "Get ConnectionManager slot snapshot", Category: "network"}
	activeConnsInfo := CommandInfo{Name: "getActiveConnections", Description: "Get all live peer connections (inbound + outbound)", Category: "network"}
	listPeersInfo := CommandInfo{Name: "listPeers", Description: "Get all known peers with exclude reasons", Category: "network"}
	listBannedInfo := CommandInfo{Name: "listBanned", Description: "Get banned IPs with reasons", Category: "network"}

	if cd == nil {
		t.RegisterUnavailable(activePeersInfo)
		t.RegisterUnavailable(activeConnsInfo)
		t.RegisterUnavailable(listPeersInfo)
		t.RegisterUnavailable(listBannedInfo)
		return
	}

	t.Register(activePeersInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			data, err := cd.ActivePeersJSON()
			if err != nil {
				return internalError(fmt.Errorf("active peers: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(activeConnsInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			data, err := cd.ActiveConnectionsJSON()
			if err != nil {
				return internalError(fmt.Errorf("active connections: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(listPeersInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			data, err := cd.ListPeersJSON()
			if err != nil {
				return internalError(fmt.Errorf("list peers: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(listBannedInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			data, err := cd.ListBannedJSON()
			if err != nil {
				return internalError(fmt.Errorf("list banned: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)
}

// RegisterMeshCommands registers relay and routing diagnostic commands.
func RegisterMeshCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "fetchRelayStatus", Description: "Get hop-by-hop relay subsystem status (active states, capable peers)", Category: "mesh"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_relay_status"})
			return frameResponse(reply)
		},
	)
}

// RegisterMetricsCommands registers fetch_traffic_history.
// When the metrics provider is nil (nodes that don't run a collector),
// the command is registered as unavailable — hidden from help/autocomplete
// and returning 503 on execution, consistent with other mode-gated commands.
func RegisterMetricsCommands(t *CommandTable, m MetricsProvider) {
	trafficHistoryInfo := CommandInfo{Name: "fetchTrafficHistory", Description: "Get rolling traffic history (1 sample/sec, 1 hour window)", Category: "metrics"}

	if m == nil {
		t.RegisterUnavailable(trafficHistoryInfo)
		return
	}

	t.Register(trafficHistoryInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := m.TrafficSnapshot()
			return frameResponse(reply)
		},
	)
}

// RegisterIdentityCommands registers fetchIdentities, fetchContacts, fetchTrustedContacts.
func RegisterIdentityCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "fetchIdentities", Description: "Fetch all known identities", Category: "identity"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_identities"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetchContacts", Description: "Fetch all contacts", Category: "identity"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetchTrustedContacts", Description: "Fetch trusted contacts", Category: "identity"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "deleteTrustedContact", Description: "Remove a trusted contact by address", Category: "identity", Usage: "<address>"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			address, _ := req.Args["address"].(string)
			if strings.TrimSpace(address) == "" {
				return validationError(fmt.Errorf("address is required"))
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "delete_trusted_contact", Address: address})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "importContacts", Description: "Import contacts from a list of address/pubkey/boxkey entries", Category: "identity"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			frame, err := frameFromArgs("import_contacts", req.Args)
			if err != nil {
				return validationError(err)
			}
			if len(frame.Contacts) == 0 {
				return validationError(fmt.Errorf("contacts array is required"))
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)
}

// RegisterMessageCommands registers message-related commands.
// Commands requiring dmRouter are only registered when it is non-nil.
// chatlog is optional — when non-nil, send_dm validates reply_to references
// synchronously against the conversation history before queueing.
func RegisterMessageCommands(t *CommandTable, node NodeProvider, dmRouter DMRouterProvider, chatlog ChatlogProvider) {
	t.Register(
		CommandInfo{Name: "fetchMessages", Description: "Fetch messages from a topic (supports limit, offset via JSON)", Category: "message", Usage: "[topic]"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			topic, _ := req.Args["topic"].(string)
			if strings.TrimSpace(topic) == "" {
				return validationError(fmt.Errorf("topic is required"))
			}
			frame := protocol.Frame{Type: "fetch_messages", Topic: topic}
			if limit, ok := numericArg(req.Args, "limit"); ok {
				frame.Limit = limit
			}
			if offset, ok := numericArg(req.Args, "offset"); ok {
				frame.Count = offset
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)

	t.Register(
		CommandInfo{Name: "fetchMessageIds", Description: "Fetch message IDs from a topic (supports limit, offset via JSON)", Category: "message", Usage: "[topic]"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			topic, _ := req.Args["topic"].(string)
			if strings.TrimSpace(topic) == "" {
				return validationError(fmt.Errorf("topic is required"))
			}
			frame := protocol.Frame{Type: "fetch_message_ids", Topic: topic}
			if limit, ok := numericArg(req.Args, "limit"); ok {
				frame.Limit = limit
			}
			if offset, ok := numericArg(req.Args, "offset"); ok {
				frame.Count = offset
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)

	t.Register(
		CommandInfo{Name: "fetchMessage", Description: "Fetch a specific message", Category: "message", Usage: "<topic> <id>"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			topic, _ := req.Args["topic"].(string)
			id, _ := req.Args["id"].(string)
			if strings.TrimSpace(topic) == "" {
				return validationError(fmt.Errorf("topic is required"))
			}
			if strings.TrimSpace(id) == "" {
				return validationError(fmt.Errorf("id is required"))
			}
			return frameResponse(node.HandleLocalFrame(protocol.Frame{
				Type:  "fetch_message",
				Topic: topic,
				ID:    id,
			}))
		},
	)

	t.Register(
		CommandInfo{Name: "fetchInbox", Description: "Fetch inbox for a recipient (defaults to self; supports limit, offset via JSON)", Category: "message", Usage: "[topic] [recipient]"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			topic, _ := req.Args["topic"].(string)
			if strings.TrimSpace(topic) == "" {
				return validationError(fmt.Errorf("topic is required"))
			}
			recipient, _ := req.Args["recipient"].(string)
			if strings.TrimSpace(recipient) == "" {
				recipient = node.Address()
			}
			frame := protocol.Frame{Type: "fetch_inbox", Topic: topic, Recipient: recipient}
			if limit, ok := numericArg(req.Args, "limit"); ok {
				frame.Limit = limit
			}
			if offset, ok := numericArg(req.Args, "offset"); ok {
				frame.Count = offset
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)

	t.Register(
		CommandInfo{Name: "fetchPendingMessages", Description: "Fetch pending messages", Category: "message", Usage: "[topic]"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			topic, _ := req.Args["topic"].(string)
			if strings.TrimSpace(topic) == "" {
				return validationError(fmt.Errorf("topic is required"))
			}
			return frameResponse(node.HandleLocalFrame(protocol.Frame{
				Type:  "fetch_pending_messages",
				Topic: topic,
			}))
		},
	)

	t.Register(
		CommandInfo{Name: "fetchDeliveryReceipts", Description: "Fetch delivery receipts (defaults to self)", Category: "message", Usage: "[recipient]"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			recipient, _ := req.Args["recipient"].(string)
			if strings.TrimSpace(recipient) == "" {
				recipient = node.Address()
			}
			return frameResponse(node.HandleLocalFrame(protocol.Frame{
				Type:      "fetch_delivery_receipts",
				Recipient: recipient,
			}))
		},
	)

	// fetch_dm_headers uses only node.HandleLocalFrame — always available.
	t.Register(
		CommandInfo{Name: "fetchDmHeaders", Description: "Fetch direct message headers", Category: "message"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			return frameResponse(node.HandleLocalFrame(protocol.Frame{
				Type: "fetch_dm_headers",
			}))
		},
	)

	deleteDMInfo := CommandInfo{Name: "deleteDm", Description: "Delete a previously sent or received DM (locally and on the peer if the message flag allows)", Category: "message", Usage: "<peer> <message_id>"}
	if dmRouter != nil {
		t.Register(deleteDMInfo,
			func(req CommandRequest) CommandResponse {
				if r, done := ctxDone(req); done {
					return r
				}
				peer, _ := req.Args["peer"].(string)
				if strings.TrimSpace(peer) == "" {
					return validationError(fmt.Errorf("peer is required"))
				}
				messageID, _ := req.Args["message_id"].(string)
				if strings.TrimSpace(messageID) == "" {
					return validationError(fmt.Errorf("message_id is required"))
				}
				targetID := domain.MessageID(messageID)
				if !targetID.IsValid() {
					return validationError(fmt.Errorf("message_id must be a valid UUID v4"))
				}
				if err := dmRouter.SendMessageDelete(req.Ctx, domain.PeerIdentity(peer), targetID); err != nil {
					return internalError(fmt.Errorf("delete dm: %w", err))
				}
				return jsonResponse(map[string]interface{}{
					"status":     "pending",
					"message":    "delete request dispatched; local row is kept for an outgoing DM until the peer's ack confirms a successful deletion (incoming local-only deletes complete synchronously)",
					"peer":       peer,
					"message_id": messageID,
				})
			},
		)
	} else {
		t.RegisterUnavailable(deleteDMInfo)
	}

	sendDMInfo := CommandInfo{Name: "sendDm", Description: "Send a direct message", Category: "message", Usage: "<to> <body>"}
	if dmRouter != nil {
		t.Register(sendDMInfo,
			func(req CommandRequest) CommandResponse {
				if r, done := ctxDone(req); done {
					return r
				}
				to, _ := req.Args["to"].(string)
				body, _ := req.Args["body"].(string)
				if strings.TrimSpace(to) == "" {
					return validationError(fmt.Errorf("to is required"))
				}
				if strings.TrimSpace(body) == "" {
					return validationError(fmt.Errorf("body is required"))
				}
				var replyTo string
				if raw, exists := req.Args["reply_to"]; exists {
					s, ok := raw.(string)
					if !ok {
						return validationError(fmt.Errorf("reply_to must be a string"))
					}
					replyTo = s
				}
				replyToID := domain.MessageID(replyTo)
				if !replyToID.IsValidOrEmpty() {
					return validationError(fmt.Errorf("reply_to must be a valid message ID (UUID v4)"))
				}
				if replyToID != "" && chatlog != nil {
					if !chatlog.HasEntryInConversation(to, string(replyToID)) {
						return validationError(fmt.Errorf("reply_to references a message that does not exist in this conversation"))
					}
				}
				if err := dmRouter.SendMessage(domain.PeerIdentity(to), domain.OutgoingDM{
					Body:    body,
					ReplyTo: replyToID,
				}); err != nil {
					// Wipe-pending barrier is a transient
					// service-state rejection, not a malformed
					// request — surface it as 503 so RPC clients
					// know they can retry once the wipe ends
					// (the barrier lifts automatically on success
					// ack / abandonment).
					if errors.Is(err, service.ErrConversationDeleteInflight) {
						return unavailableError(fmt.Errorf("send message: %w", err))
					}
					return validationError(fmt.Errorf("send message: %w", err))
				}
				return jsonResponse(map[string]interface{}{
					"status":  "pending",
					"message": "message queued for delivery; actual send is asynchronous",
					"to":      to,
				})
			},
		)
	} else {
		t.RegisterUnavailable(sendDMInfo)
	}

	// Low-level message storage commands. These accept raw protocol frame fields
	// and delegate directly to HandleLocalFrame. Used by internal tooling and
	// raw JSON console input; normal users should prefer send_dm.
	t.Register(
		CommandInfo{Name: "sendMessage", Description: "Store an incoming message (raw frame)", Category: "message"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			frame, err := frameFromArgs("send_message", req.Args)
			if err != nil {
				return validationError(err)
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)

	t.Register(
		CommandInfo{Name: "importMessage", Description: "Import a message without delivery side-effects (raw frame)", Category: "message"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			frame, err := frameFromArgs("import_message", req.Args)
			if err != nil {
				return validationError(err)
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)

	t.Register(
		CommandInfo{Name: "sendDeliveryReceipt", Description: "Store a delivery receipt (raw frame)", Category: "message"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			frame, err := frameFromArgs("send_delivery_receipt", req.Args)
			if err != nil {
				return validationError(err)
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)
}

// registerFileAnnounceCommand registers the send_file_announce RPC command.
// The command constructs a file_announce DM — a user-visible message stored
// in chatlog with delivery receipts and gossip fallback, carrying file
// metadata (name, size, hash, content type) inside the encrypted envelope.
func registerFileAnnounceCommand(t *CommandTable, node NodeProvider, dmRouter DMRouterProvider) {
	info := CommandInfo{
		Name:        "sendFileAnnounce",
		Description: "Announce a file for transfer (sends file_announce DM)",
		Category:    "file",
		Usage:       "<to> <file_name> <file_size> <file_hash> [content_type] [body]",
	}

	if dmRouter == nil || node == nil {
		t.RegisterUnavailable(info)
		return
	}

	t.Register(info, func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		to, _ := req.Args["to"].(string)
		if strings.TrimSpace(to) == "" {
			return validationError(fmt.Errorf("to is required"))
		}

		fileName, _ := req.Args["file_name"].(string)
		if strings.TrimSpace(fileName) == "" {
			return validationError(fmt.Errorf("file_name is required"))
		}

		fileSizeRaw, ok := req.Args["file_size"]
		if !ok {
			return validationError(fmt.Errorf("file_size is required"))
		}
		fileSize, err := parseUint64Arg(fileSizeRaw)
		if err != nil {
			return validationError(fmt.Errorf("file_size: %w", err))
		}
		if fileSize == 0 {
			return validationError(fmt.Errorf("file_size must be greater than zero"))
		}

		fileHash, _ := req.Args["file_hash"].(string)
		if strings.TrimSpace(fileHash) == "" {
			return validationError(fmt.Errorf("file_hash is required"))
		}
		if err := domain.ValidateFileHash(fileHash); err != nil {
			return validationError(fmt.Errorf("file_hash: %w", err))
		}

		contentType, _ := req.Args["content_type"].(string)
		if strings.TrimSpace(contentType) == "" {
			contentType = "application/octet-stream"
		}

		body, _ := req.Args["body"].(string)
		if strings.TrimSpace(body) == "" {
			body = domain.FileDMBodySentinel
		}

		announcePayload := domain.FileAnnouncePayload{
			FileName:    fileName,
			FileSize:    fileSize,
			ContentType: contentType,
			FileHash:    fileHash,
		}
		commandData, err := json.Marshal(announcePayload)
		if err != nil {
			return internalError(fmt.Errorf("marshal file announce payload: %w", err))
		}

		// Validate the transmit file and reserve a sender quota slot
		// synchronously. The actual DM delivery and sender mapping
		// registration happen asynchronously inside SendFileAnnounce —
		// the response reflects only pre-send validation, not delivery.
		if err := dmRouter.SendFileAnnounce(domain.PeerIdentity(to), domain.OutgoingDM{
			Body:        body,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: string(commandData),
		}, domain.FileAnnouncePayload{
			FileHash:    fileHash,
			FileName:    fileName,
			FileSize:    fileSize,
			ContentType: contentType,
		}, nil); err != nil {
			// Wipe-pending barrier — same 503 mapping as
			// send_dm so RPC clients see a transient
			// rejection (retry once the wipe ends) rather
			// than a 500 Internal Server Error.
			if errors.Is(err, service.ErrConversationDeleteInflight) {
				return unavailableError(fmt.Errorf("file announce failed: %w", err))
			}
			return internalError(fmt.Errorf("file announce failed: %w", err))
		}

		return jsonResponse(map[string]interface{}{
			"status":    "pending",
			"message":   "file announce validated and queued for delivery; actual send is asynchronous",
			"to":        to,
			"file_name": fileName,
			"file_size": fileSize,
			"file_hash": fileHash,
		})
	})
}

// parseUint64Arg converts a value that may arrive as float64 (JSON path) or
// string (key=value / positional) to uint64.
func parseUint64Arg(v interface{}) (uint64, error) {
	switch n := v.(type) {
	case float64:
		if n < 0 {
			return 0, fmt.Errorf("must be non-negative")
		}
		return uint64(n), nil
	case string:
		return strconv.ParseUint(n, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

// RegisterFileCommands registers file transfer commands.
// Includes send_file_announce plus observability commands for active transfers.
func RegisterFileCommands(t *CommandTable, node NodeProvider, dmRouter DMRouterProvider) {
	registerFileAnnounceCommand(t, node, dmRouter)

	transfersInfo := CommandInfo{Name: "fetchFileTransfers", Description: "List all active/pending file transfers", Category: "file"}
	allTransfersInfo := CommandInfo{Name: "fetchAllFileTransfers", Description: "List ALL file transfers including terminal states (completed, failed, tombstone) — powers the desktop file tab history", Category: "file"}
	mappingInfo := CommandInfo{Name: "fetchFileMapping", Description: "Show sender FileMapping table (no TransmitPath)", Category: "file"}
	retryInfo := CommandInfo{Name: "retryFileChunk", Description: "Force retry current pending chunk request", Category: "file", Usage: "<file_id>"}
	startInfo := CommandInfo{Name: "startFileDownload", Description: "Start downloading a previously announced file", Category: "file", Usage: "<file_id>"}
	cancelInfo := CommandInfo{Name: "cancelFileDownload", Description: "Cancel an active download and delete partial data", Category: "file", Usage: "<file_id>"}
	restartInfo := CommandInfo{Name: "restartFileDownload", Description: "Reset a failed download back to available for re-download", Category: "file", Usage: "<file_id>"}
	explainInfo := CommandInfo{Name: "explainFileRoute", Description: "Explain ranked next-hop plan a file command would use for the given destination identity (best, hops, protocol_version, connected_at, uptime_seconds)", Category: "file", Usage: "<identity>"}

	if node == nil {
		t.RegisterUnavailable(transfersInfo)
		t.RegisterUnavailable(allTransfersInfo)
		t.RegisterUnavailable(mappingInfo)
		t.RegisterUnavailable(retryInfo)
		t.RegisterUnavailable(startInfo)
		t.RegisterUnavailable(cancelInfo)
		t.RegisterUnavailable(restartInfo)
		t.RegisterUnavailable(explainInfo)
		return
	}

	t.Register(transfersInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			data, err := node.FetchFileTransfers()
			if err != nil {
				return internalError(fmt.Errorf("fetch file transfers: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(allTransfersInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			data, err := node.FetchAllFileTransfers()
			if err != nil {
				return internalError(fmt.Errorf("fetch all file transfers: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(mappingInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			data, err := node.FetchFileMappings()
			if err != nil {
				return internalError(fmt.Errorf("fetch file mappings: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(retryInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			fileID, _ := req.Args["file_id"].(string)
			if strings.TrimSpace(fileID) == "" {
				return validationError(fmt.Errorf("file_id is required"))
			}
			if err := node.RetryFileChunk(domain.FileID(fileID)); err != nil {
				return internalError(fmt.Errorf("retry file chunk: %w", err))
			}
			return jsonResponse(map[string]interface{}{"status": "retried", "file_id": fileID})
		},
	)

	t.Register(startInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			fileID, _ := req.Args["file_id"].(string)
			if strings.TrimSpace(fileID) == "" {
				return validationError(fmt.Errorf("file_id is required"))
			}
			if err := node.StartFileDownload(domain.FileID(fileID)); err != nil {
				return internalError(fmt.Errorf("start file download: %w", err))
			}
			return jsonResponse(map[string]interface{}{"status": "downloading", "file_id": fileID})
		},
	)

	t.Register(cancelInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			fileID, _ := req.Args["file_id"].(string)
			if strings.TrimSpace(fileID) == "" {
				return validationError(fmt.Errorf("file_id is required"))
			}
			if err := node.CancelFileDownload(domain.FileID(fileID)); err != nil {
				return internalError(fmt.Errorf("cancel file download: %w", err))
			}
			return jsonResponse(map[string]interface{}{"status": "cancelled", "file_id": fileID})
		},
	)

	t.Register(restartInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			fileID, _ := req.Args["file_id"].(string)
			if strings.TrimSpace(fileID) == "" {
				return validationError(fmt.Errorf("file_id is required"))
			}
			if err := node.RestartFileDownload(domain.FileID(fileID)); err != nil {
				return internalError(fmt.Errorf("restart file download: %w", err))
			}
			return jsonResponse(map[string]interface{}{"status": "restarted", "file_id": fileID})
		},
	)

	t.Register(explainInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			identityArg, _ := req.Args["identity"].(string)
			identityArg = strings.TrimSpace(identityArg)
			if identityArg == "" {
				return validationError(fmt.Errorf("identity is required"))
			}
			// Validate the destination address up-front so a typo from the
			// console does not leak into the file router as an empty/garbled
			// PeerIdentity. ValidateAddress is the same gate fetchRouteLookup
			// uses, keeping the two diagnostic surfaces consistent.
			if err := identity.ValidateAddress(identityArg); err != nil {
				return validationError(fmt.Errorf("invalid identity format: %w", err))
			}
			data, err := node.ExplainFileRoute(domain.PeerIdentity(identityArg))
			if err != nil {
				return internalError(fmt.Errorf("explain file route: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)
}

// RegisterChatlogCommands registers chatlog-related commands.
// When chatlog provider is nil, commands are registered as unavailable (503).
func RegisterChatlogCommands(t *CommandTable, chatlog ChatlogProvider) {
	chatlogInfo := CommandInfo{Name: "fetchChatlog", Description: "Fetch chatlog entries (defaults: topic=dm, all peers)", Category: "chatlog", Usage: "[topic] [peer_address]"}
	previewsInfo := CommandInfo{Name: "fetchChatlogPreviews", Description: "Fetch chatlog previews", Category: "chatlog"}
	conversationsInfo := CommandInfo{Name: "fetchConversations", Description: "Fetch conversations", Category: "chatlog"}

	if chatlog == nil {
		t.RegisterUnavailable(chatlogInfo)
		t.RegisterUnavailable(previewsInfo)
		t.RegisterUnavailable(conversationsInfo)
		return
	}

	t.Register(chatlogInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			topic, _ := req.Args["topic"].(string)
			peerAddress, _ := req.Args["peer_address"].(string)
			if strings.TrimSpace(topic) == "" {
				topic = "dm"
			}
			// peer_address is optional: empty string returns all entries for the topic,
			// matching the old desktop console behavior (consoleFetchChatlog).
			entries, err := chatlog.FetchChatlog(topic, peerAddress)
			if err != nil {
				return internalError(fmt.Errorf("fetch chatlog: %w", err))
			}
			return rawJSONResponse(entries)
		},
	)

	t.Register(previewsInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			previews, err := chatlog.FetchChatlogPreviews()
			if err != nil {
				return internalError(fmt.Errorf("fetch chatlog previews: %w", err))
			}
			return rawJSONResponse(previews)
		},
	)

	t.Register(conversationsInfo,
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			conversations, err := chatlog.FetchConversations()
			if err != nil {
				return internalError(fmt.Errorf("fetch conversations: %w", err))
			}
			return rawJSONResponse(conversations)
		},
	)
}

// RegisterNoticeCommands registers notice-related commands.
func RegisterNoticeCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "fetchNotices", Description: "Fetch all notices", Category: "notice"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			return frameResponse(node.HandleLocalFrame(protocol.Frame{Type: "fetch_notices"}))
		},
	)

	t.Register(
		CommandInfo{Name: "publishNotice", Description: "Publish an encrypted notice with TTL (raw frame)", Category: "notice"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			frame, err := frameFromArgs("publish_notice", req.Args)
			if err != nil {
				return validationError(err)
			}
			if strings.TrimSpace(frame.Ciphertext) == "" {
				return validationError(fmt.Errorf("ciphertext is required"))
			}
			if frame.TTLSeconds <= 0 {
				return validationError(fmt.Errorf("ttl_seconds must be positive"))
			}
			return frameResponse(node.HandleLocalFrame(frame))
		},
	)
}

// RegisterDesktopOverrides registers the transport-level hello override when
// a DiagnosticProvider is available.
//
// Raw commands (ping, getPeers) are NOT overridden. They retain their base
// semantics regardless of whether the node runs in desktop or standalone
// mode. This ensures "one name = one contract" across all transports.
//
// The hello override is a transport-level concern (Client: "desktop" vs
// Client: "rpc") and does not violate the taxonomy — it changes identity
// metadata, not the semantic contract of the command.
//
// Call this AFTER RegisterAllCommands. Pass nil diag to skip (standalone node).
// Both diag and node must be non-nil when overrides are enabled — the hello
// override calls node.HandleLocalFrame to send the enriched identity frame.
func RegisterDesktopOverrides(t *CommandTable, diag DiagnosticProvider, node NodeProvider) {
	if diag == nil {
		return
	}
	if node == nil {
		panic("rpc: RegisterDesktopOverrides requires non-nil NodeProvider when DiagnosticProvider is set")
	}

	// --- Transport-level hello override (not a semantic fork) ---

	t.Register(
		CommandInfo{Name: "hello", Description: "Send hello frame to identify with peers", Category: "system"},
		func(req CommandRequest) CommandResponse {
			if r, done := ctxDone(req); done {
				return r
			}
			reply := node.HandleLocalFrame(protocol.Frame{
				Type:                   "hello",
				Version:                config.ProtocolVersion,
				MinimumProtocolVersion: config.MinimumProtocolVersion,
				Client:                 "desktop",
				ClientVersion:          strings.ReplaceAll(diag.DesktopVersion(), " ", "-"),
				ClientBuild:            config.ClientBuild,
			})
			return frameResponse(reply)
		},
	)
}
