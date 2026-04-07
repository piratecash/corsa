package rpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// CommandRequest holds the parsed input for a command execution.
type CommandRequest struct {
	Name string
	Args map[string]interface{}
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
	unavailable map[string]bool // mode-gated commands registered as unavailable
}

// NewCommandTable creates an empty command table.
func NewCommandTable() *CommandTable {
	return &CommandTable{
		handlers:    make(map[string]CommandHandler),
		metadata:    make(map[string]CommandInfo),
		unavailable: make(map[string]bool),
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

// Execute runs a command by name and returns the response.
func (t *CommandTable) Execute(req CommandRequest) CommandResponse {
	t.mu.RLock()
	handler, exists := t.handlers[req.Name]
	t.mu.RUnlock()

	if !exists {
		return CommandResponse{
			Error:     fmt.Errorf("unknown command: %s", req.Name),
			ErrorKind: ErrNotFound,
		}
	}
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

// Has returns true if the command is registered.
func (t *CommandTable) Has(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.handlers[name]
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
func RegisterAllCommands(t *CommandTable, node NodeProvider, chatlog ChatlogProvider, dmRouter DMRouterProvider, metricsProvider MetricsProvider, routingProvider RoutingProvider) {
	RegisterSystemCommands(t, node)
	RegisterNetworkCommands(t, node)
	RegisterMeshCommands(t, node)
	RegisterIdentityCommands(t, node)
	RegisterMessageCommands(t, node, dmRouter, chatlog)
	RegisterFileCommands(t, node, dmRouter)
	RegisterChatlogCommands(t, chatlog)
	RegisterNoticeCommands(t, node)
	RegisterMetricsCommands(t, metricsProvider)
	RegisterRoutingCommands(t, routingProvider)
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

// --- Registration helpers for common patterns ---

// helpSchemaVersion is the version of the help response format.
// Bump when the structure of the help response changes (e.g. new fields
// in CommandInfo). This is NOT the protocol or client version — those
// are returned by the separate "version" command.
const helpSchemaVersion = "1.0"

// RegisterSystemCommands registers help, ping, hello, version.
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
			reply := node.HandleLocalFrame(protocol.Frame{Type: "ping"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "hello", Description: "Send hello frame to identify with peers", Category: "system"},
		func(req CommandRequest) CommandResponse {
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
}

// RegisterNetworkCommands registers get_peers, fetch_peer_health, add_peer.
func RegisterNetworkCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "get_peers", Description: "Get list of connected peers", Category: "network"},
		func(req CommandRequest) CommandResponse {
			reply := node.HandleLocalFrame(protocol.Frame{Type: "get_peers"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetch_peer_health", Description: "Get peer health status", Category: "network"},
		func(req CommandRequest) CommandResponse {
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetch_network_stats", Description: "Get aggregated network traffic statistics per peer and total", Category: "network"},
		func(req CommandRequest) CommandResponse {
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "add_peer", Description: "Add a new peer by address", Category: "network", Usage: "<address>"},
		func(req CommandRequest) CommandResponse {
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
}

// RegisterMeshCommands registers relay and routing diagnostic commands.
func RegisterMeshCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "fetch_relay_status", Description: "Get hop-by-hop relay subsystem status (active states, capable peers)", Category: "mesh"},
		func(req CommandRequest) CommandResponse {
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
	trafficHistoryInfo := CommandInfo{Name: "fetch_traffic_history", Description: "Get rolling traffic history (1 sample/sec, 1 hour window)", Category: "metrics"}

	if m == nil {
		t.RegisterUnavailable(trafficHistoryInfo)
		return
	}

	t.Register(trafficHistoryInfo,
		func(req CommandRequest) CommandResponse {
			reply := m.TrafficSnapshot()
			return frameResponse(reply)
		},
	)
}

// RegisterIdentityCommands registers fetch_identities, fetch_contacts, fetch_trusted_contacts.
func RegisterIdentityCommands(t *CommandTable, node NodeProvider) {
	t.Register(
		CommandInfo{Name: "fetch_identities", Description: "Fetch all known identities", Category: "identity"},
		func(req CommandRequest) CommandResponse {
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_identities"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetch_contacts", Description: "Fetch all contacts", Category: "identity"},
		func(req CommandRequest) CommandResponse {
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "fetch_trusted_contacts", Description: "Fetch trusted contacts", Category: "identity"},
		func(req CommandRequest) CommandResponse {
			reply := node.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
			return frameResponse(reply)
		},
	)

	t.Register(
		CommandInfo{Name: "delete_trusted_contact", Description: "Remove a trusted contact by address", Category: "identity", Usage: "<address>"},
		func(req CommandRequest) CommandResponse {
			address, _ := req.Args["address"].(string)
			if strings.TrimSpace(address) == "" {
				return validationError(fmt.Errorf("address is required"))
			}
			reply := node.HandleLocalFrame(protocol.Frame{Type: "delete_trusted_contact", Address: address})
			return frameResponse(reply)
		},
	)
}

// RegisterMessageCommands registers message-related commands.
// Commands requiring dmRouter are only registered when it is non-nil.
// chatlog is optional — when non-nil, send_dm validates reply_to references
// synchronously against the conversation history before queueing.
func RegisterMessageCommands(t *CommandTable, node NodeProvider, dmRouter DMRouterProvider, chatlog ChatlogProvider) {
	t.Register(
		CommandInfo{Name: "fetch_messages", Description: "Fetch messages from a topic (supports limit, offset via JSON)", Category: "message", Usage: "[topic]"},
		func(req CommandRequest) CommandResponse {
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
		CommandInfo{Name: "fetch_message_ids", Description: "Fetch message IDs from a topic (supports limit, offset via JSON)", Category: "message", Usage: "[topic]"},
		func(req CommandRequest) CommandResponse {
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
		CommandInfo{Name: "fetch_message", Description: "Fetch a specific message", Category: "message", Usage: "<topic> <id>"},
		func(req CommandRequest) CommandResponse {
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
		CommandInfo{Name: "fetch_inbox", Description: "Fetch inbox for a recipient (defaults to self; supports limit, offset via JSON)", Category: "message", Usage: "[topic] [recipient]"},
		func(req CommandRequest) CommandResponse {
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
		CommandInfo{Name: "fetch_pending_messages", Description: "Fetch pending messages", Category: "message", Usage: "[topic]"},
		func(req CommandRequest) CommandResponse {
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
		CommandInfo{Name: "fetch_delivery_receipts", Description: "Fetch delivery receipts (defaults to self)", Category: "message", Usage: "[recipient]"},
		func(req CommandRequest) CommandResponse {
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
		CommandInfo{Name: "fetch_dm_headers", Description: "Fetch direct message headers", Category: "message"},
		func(req CommandRequest) CommandResponse {
			return frameResponse(node.HandleLocalFrame(protocol.Frame{
				Type: "fetch_dm_headers",
			}))
		},
	)

	sendDMInfo := CommandInfo{Name: "send_dm", Description: "Send a direct message", Category: "message", Usage: "<to> <body>"}
	if dmRouter != nil {
		t.Register(sendDMInfo,
			func(req CommandRequest) CommandResponse {
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
				dmRouter.SendMessage(domain.PeerIdentity(to), domain.OutgoingDM{
					Body:    body,
					ReplyTo: replyToID,
				})
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
}

// registerFileAnnounceCommand registers the send_file_announce RPC command.
// The command constructs a file_announce DM — a user-visible message stored
// in chatlog with delivery receipts and gossip fallback, carrying file
// metadata (name, size, hash, content type) inside the encrypted envelope.
func registerFileAnnounceCommand(t *CommandTable, node NodeProvider, dmRouter DMRouterProvider) {
	info := CommandInfo{
		Name:        "send_file_announce",
		Description: "Announce a file for transfer (sends file_announce DM)",
		Category:    "file",
		Usage:       "<to> <file_name> <file_size> <file_hash> [content_type] [body]",
	}

	if dmRouter == nil || node == nil {
		t.RegisterUnavailable(info)
		return
	}

	t.Register(info, func(req CommandRequest) CommandResponse {
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
			Command:     domain.FileActionAnnounce,
			CommandData: string(commandData),
		}, domain.FileAnnouncePayload{
			FileHash:    fileHash,
			FileName:    fileName,
			FileSize:    fileSize,
			ContentType: contentType,
		}, nil); err != nil {
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

	transfersInfo := CommandInfo{Name: "fetch_file_transfers", Description: "List all active/pending file transfers", Category: "file"}
	mappingInfo := CommandInfo{Name: "fetch_file_mapping", Description: "Show sender FileMapping table (no TransmitPath)", Category: "file"}
	retryInfo := CommandInfo{Name: "retry_file_chunk", Description: "Force retry current pending chunk request", Category: "file", Usage: "<file_id>"}
	startInfo := CommandInfo{Name: "start_file_download", Description: "Start downloading a previously announced file", Category: "file", Usage: "<file_id>"}
	cancelInfo := CommandInfo{Name: "cancel_file_download", Description: "Cancel an active download and delete partial data", Category: "file", Usage: "<file_id>"}
	restartInfo := CommandInfo{Name: "restart_file_download", Description: "Reset a failed download back to available for re-download", Category: "file", Usage: "<file_id>"}

	if node == nil {
		t.RegisterUnavailable(transfersInfo)
		t.RegisterUnavailable(mappingInfo)
		t.RegisterUnavailable(retryInfo)
		t.RegisterUnavailable(startInfo)
		t.RegisterUnavailable(cancelInfo)
		t.RegisterUnavailable(restartInfo)
		return
	}

	t.Register(transfersInfo,
		func(req CommandRequest) CommandResponse {
			data, err := node.FetchFileTransfers()
			if err != nil {
				return internalError(fmt.Errorf("fetch file transfers: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(mappingInfo,
		func(req CommandRequest) CommandResponse {
			data, err := node.FetchFileMappings()
			if err != nil {
				return internalError(fmt.Errorf("fetch file mappings: %w", err))
			}
			return CommandResponse{Data: data}
		},
	)

	t.Register(retryInfo,
		func(req CommandRequest) CommandResponse {
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
}

// RegisterChatlogCommands registers chatlog-related commands.
// When chatlog provider is nil, commands are registered as unavailable (503).
func RegisterChatlogCommands(t *CommandTable, chatlog ChatlogProvider) {
	chatlogInfo := CommandInfo{Name: "fetch_chatlog", Description: "Fetch chatlog entries (defaults: topic=dm, all peers)", Category: "chatlog", Usage: "[topic] [peer_address]"}
	previewsInfo := CommandInfo{Name: "fetch_chatlog_previews", Description: "Fetch chatlog previews", Category: "chatlog"}
	conversationsInfo := CommandInfo{Name: "fetch_conversations", Description: "Fetch conversations", Category: "chatlog"}

	if chatlog == nil {
		t.RegisterUnavailable(chatlogInfo)
		t.RegisterUnavailable(previewsInfo)
		t.RegisterUnavailable(conversationsInfo)
		return
	}

	t.Register(chatlogInfo,
		func(req CommandRequest) CommandResponse {
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
			previews, err := chatlog.FetchChatlogPreviews()
			if err != nil {
				return internalError(fmt.Errorf("fetch chatlog previews: %w", err))
			}
			return rawJSONResponse(previews)
		},
	)

	t.Register(conversationsInfo,
		func(req CommandRequest) CommandResponse {
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
		CommandInfo{Name: "fetch_notices", Description: "Fetch all notices", Category: "notice"},
		func(req CommandRequest) CommandResponse {
			return frameResponse(node.HandleLocalFrame(protocol.Frame{Type: "fetch_notices"}))
		},
	)
}

// RegisterDesktopOverrides replaces base command handlers with desktop-enriched
// versions when a DiagnosticProvider is available.
//
// The base ping (system) and get_peers (network) handlers forward directly
// to HandleLocalFrame and return raw wire frames. The desktop client provides
// richer versions: ping opens TCP sessions to every connected peer and reports
// per-peer status; get_peers merges the raw list with peer health data and
// categorizes peers into connected/pending/known_only groups.
//
// The base hello handler identifies as Client: "rpc". The desktop override
// identifies as Client: "desktop" with the desktop application version,
// ensuring that peers see the correct client type in handshake frames.
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

	t.Register(
		CommandInfo{Name: "ping", Description: "Ping all connected peers and report status", Category: "system"},
		func(req CommandRequest) CommandResponse {
			output, err := diag.ConsolePingJSON()
			if err != nil {
				return internalError(fmt.Errorf("console ping: %w", err))
			}
			return rawJSONResponse(output)
		},
	)

	t.Register(
		CommandInfo{Name: "get_peers", Description: "Get peers with health status and categorization", Category: "network"},
		func(req CommandRequest) CommandResponse {
			output, err := diag.ConsolePeersJSON()
			if err != nil {
				return internalError(fmt.Errorf("console peers: %w", err))
			}
			return rawJSONResponse(output)
		},
	)

	t.Register(
		CommandInfo{Name: "hello", Description: "Send hello frame to identify with peers", Category: "system"},
		func(req CommandRequest) CommandResponse {
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
