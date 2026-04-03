package rpc

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ParseConsoleInput converts a raw console command string (e.g. "send_dm addr hello world")
// into a CommandRequest with named args that CommandTable can execute directly.
// Used by both the UI console (in-process) and rpc.Client (over HTTP via /exec).
//
// Supports three input formats:
//   - Named command:   "send_dm addr hello world" (positional args mapped to named args)
//   - Key=value:       "send_dm to=addr body=hello reply_to=a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5" (all tokens are key=value)
//   - Raw JSON frame:  {"type":"ping"} (type becomes command name, all fields become args)
//
// Key=value mode is auto-detected when every token after the command contains '='
// with a non-empty key. If any token is bare, the entire input is treated as positional.
//
// Command names are case-insensitive: "HELP", "Help", "help" all work.
func ParseConsoleInput(input string) (CommandRequest, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return CommandRequest{}, fmt.Errorf("empty command")
	}

	// Raw JSON frame: {"type": "ping", ...}
	// Extract "type" as command name, pass all fields as args.
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		return parseJSONFrame(trimmed)
	}

	// Quote-aware tokenization: handles body="hello world" as a single token.
	// Used for key=value detection. Falls back to strings.Fields for positional.
	allTokens, quoteErr := splitQuotedTokens(trimmed)
	if quoteErr != nil {
		return CommandRequest{}, quoteErr
	}
	if len(allTokens) == 0 {
		return CommandRequest{}, fmt.Errorf("empty command")
	}
	command := strings.ToLower(strings.TrimSpace(allTokens[0]))
	if command == "" {
		return CommandRequest{}, fmt.Errorf("empty command")
	}
	quotedTokens := allTokens[1:]

	if args, ok := tryParseKeyValue(quotedTokens); ok {
		return CommandRequest{Name: command, Args: args}, nil
	}

	// Positional fallback: use simple whitespace split (no quote handling)
	// to preserve backward-compatible multi-word body joining.
	fields := strings.Fields(trimmed)
	positional := fields[1:]

	args, err := mapPositionalArgs(command, positional)
	if err != nil {
		return CommandRequest{}, err
	}

	return CommandRequest{
		Name: command,
		Args: args,
	}, nil
}

// splitQuotedTokens splits input on whitespace but preserves quoted segments.
// Both double quotes and single quotes are supported. The quotes themselves
// are stripped from the returned tokens.
//
// Backslash escapes are recognized inside quoted segments: \" produces a
// literal double quote, \' a literal single quote, and \\ a literal
// backslash. Outside quotes, backslash has no special meaning.
//
// Returns an error if a quoted segment is not closed before the end of input.
func splitQuotedTokens(input string) ([]string, error) {
	var tokens []string
	var current strings.Builder
	inQuote := false
	var quoteChar byte

	for i := 0; i < len(input); i++ {
		ch := input[i]
		switch {
		case inQuote:
			if ch == '\\' && i+1 < len(input) {
				next := input[i+1]
				if next == quoteChar || next == '\\' {
					current.WriteByte(next)
					i++
				} else {
					current.WriteByte(ch)
				}
			} else if ch == quoteChar {
				inQuote = false
			} else {
				current.WriteByte(ch)
			}
		case ch == '"' || ch == '\'':
			inQuote = true
			quoteChar = ch
		case ch == ' ' || ch == '\t':
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(ch)
		}
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated %c quote", quoteChar)
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens, nil
}

// tryParseKeyValue attempts to parse all tokens as key=value pairs.
// Returns the map and true if every token has '=' with a non-empty key.
// Returns nil and false if any token is bare — caller should fall through
// to positional parsing.
//
// Values are always stored as strings. Typed JSON values (numbers, booleans,
// arrays) are handled by the JSON frame path (parseJSONFrame); key=value
// mode is a console convenience where every value is text.
func tryParseKeyValue(tokens []string) (map[string]interface{}, bool) {
	if len(tokens) == 0 {
		return nil, false
	}
	for _, tok := range tokens {
		idx := strings.IndexByte(tok, '=')
		if idx < 1 {
			return nil, false
		}
	}
	m := make(map[string]interface{}, len(tokens))
	for _, tok := range tokens {
		idx := strings.IndexByte(tok, '=')
		key := tok[:idx]
		value := tok[idx+1:]
		m[key] = value
	}
	return m, true
}

// parseJSONFrame parses a raw JSON object as a command request.
// The "type" field becomes the command name; all fields are passed as args.
//
// Protocol frame field names are normalized to RPC arg names so that users
// can paste real wire frames (e.g. {"type":"add_peer","peers":["host:port"]})
// and have them work through the RPC handler layer.
func parseJSONFrame(input string) (CommandRequest, error) {
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(input), &fields); err != nil {
		return CommandRequest{}, fmt.Errorf("invalid JSON frame: %w", err)
	}

	frameType, _ := fields["type"].(string)
	if strings.TrimSpace(frameType) == "" {
		return CommandRequest{}, fmt.Errorf("JSON frame requires a 'type' field")
	}

	name := strings.ToLower(frameType)
	normalizeFrameArgs(name, fields)

	return CommandRequest{
		Name: name,
		Args: fields,
	}, nil
}

// normalizeFrameArgs translates protocol frame field names to RPC arg names.
//
// The protocol wire format and the RPC handler contract use different field
// names for the same data. For example, the add_peer protocol frame carries
// addresses in a "peers" array, but the RPC handler expects a single "address"
// string. This function bridges that gap so users can paste raw wire frames
// into the console and have them dispatched correctly.
//
// Rules:
//   - Aliases are only applied when the RPC-expected field is absent.
//   - If both the frame field and the RPC field are present, the RPC field wins.
//   - The "count" → "offset" alias applies to all commands that support pagination.
func normalizeFrameArgs(command string, args map[string]interface{}) {
	switch command {
	case "add_peer":
		// Wire: {"peers": ["host:port"]}  →  RPC: {"address": "host:port"}
		if _, has := args["address"]; !has {
			if peers, ok := args["peers"]; ok {
				if list, ok := peers.([]interface{}); ok && len(list) > 0 {
					if addr, ok := list[0].(string); ok {
						args["address"] = addr
					}
				}
			}
		}

	case "send_dm":
		// Wire: {"recipient": "addr"}  →  RPC: {"to": "addr"}
		if _, has := args["to"]; !has {
			if recipient, ok := args["recipient"].(string); ok {
				args["to"] = recipient
			}
		}

	case "fetch_chatlog":
		// Wire: {"address": "addr"}  →  RPC: {"peer_address": "addr"}
		if _, has := args["peer_address"]; !has {
			if addr, ok := args["address"].(string); ok {
				args["peer_address"] = addr
			}
		}
	}

	// Pagination: wire uses "count", RPC handlers read "offset".
	if _, has := args["offset"]; !has {
		if count, ok := args["count"]; ok {
			args["offset"] = count
		}
	}
}

// mapPositionalArgs converts positional CLI arguments to a named args map
// based on the command's expected parameter layout.
func mapPositionalArgs(command string, args []string) (map[string]interface{}, error) {
	noArgCommands := map[string]bool{
		"help": true, "ping": true, "hello": true, "version": true,
		"get_peers": true, "fetch_peer_health": true, "fetch_network_stats": true,
		"fetch_identities": true, "fetch_contacts": true, "fetch_trusted_contacts": true,
		"fetch_notices": true, "fetch_chatlog_previews": true, "fetch_conversations": true,
		"fetch_dm_headers": true, "fetch_relay_status": true,
		"fetch_route_table": true, "fetch_route_summary": true,
	}

	if noArgCommands[command] {
		if len(args) > 0 {
			return nil, fmt.Errorf("%s takes no arguments", command)
		}
		return nil, nil
	}

	switch command {
	case "add_peer":
		if len(args) < 1 {
			return nil, fmt.Errorf("add_peer requires address argument")
		}
		return map[string]interface{}{"address": args[0]}, nil

	case "delete_trusted_contact":
		if len(args) < 1 {
			return nil, fmt.Errorf("delete_trusted_contact requires address argument")
		}
		return map[string]interface{}{"address": args[0]}, nil

	case "fetch_messages":
		topic := stringArgOrDefault(args, 0, "global")
		return map[string]interface{}{"topic": topic}, nil

	case "fetch_message_ids":
		topic := stringArgOrDefault(args, 0, "global")
		return map[string]interface{}{"topic": topic}, nil

	case "fetch_message":
		if len(args) < 2 {
			return nil, fmt.Errorf("fetch_message requires topic and id arguments")
		}
		return map[string]interface{}{"topic": args[0], "id": strings.Join(args[1:], " ")}, nil

	case "fetch_inbox":
		topic := stringArgOrDefault(args, 0, "dm")
		result := map[string]interface{}{"topic": topic}
		if len(args) > 1 {
			result["recipient"] = args[1]
		}
		return result, nil

	case "fetch_pending_messages":
		topic := stringArgOrDefault(args, 0, "dm")
		return map[string]interface{}{"topic": topic}, nil

	case "fetch_delivery_receipts":
		result := map[string]interface{}{}
		if len(args) > 0 {
			result["recipient"] = args[0]
		}
		return result, nil

	case "fetch_chatlog":
		topic := stringArgOrDefault(args, 0, "dm")
		result := map[string]interface{}{"topic": topic}
		if len(args) > 1 {
			result["peer_address"] = args[1]
		}
		return result, nil

	case "send_dm":
		if len(args) < 2 {
			return nil, fmt.Errorf("send_dm requires to and body arguments")
		}
		return map[string]interface{}{"to": args[0], "body": strings.Join(args[1:], " ")}, nil

	case "fetch_route_lookup":
		if len(args) < 1 {
			return nil, fmt.Errorf("fetch_route_lookup requires identity argument")
		}
		if len(args) > 1 {
			return nil, fmt.Errorf("fetch_route_lookup takes exactly one argument")
		}
		return map[string]interface{}{"identity": args[0]}, nil
	}

	// Unknown command — pass through with no args, let CommandTable handle the error
	return nil, nil
}

func stringArgOrDefault(args []string, index int, fallback string) string {
	if len(args) <= index {
		return fallback
	}
	return args[index]
}
