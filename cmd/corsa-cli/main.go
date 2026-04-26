package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/rpc"
)

func main() {
	// Flag defaults fall back to the same CORSA_RPC_* environment variables
	// the node reads (see internal/core/config.LoadFromEnv). Running
	// corsa-cli inside the docker image — where these vars are exported for
	// the node — must work without re-passing credentials on the command
	// line. Explicit flags still win over the environment.
	host := flag.String("host", envOrDefault("CORSA_RPC_HOST", "127.0.0.1"), "RPC server host (env: CORSA_RPC_HOST)")
	port := flag.String("port", envOrDefault("CORSA_RPC_PORT", "46464"), "RPC server port (env: CORSA_RPC_PORT)")
	username := flag.String("username", envOrDefault("CORSA_RPC_USERNAME", ""), "RPC username (env: CORSA_RPC_USERNAME)")
	password := flag.String("password", envOrDefault("CORSA_RPC_PASSWORD", ""), "RPC password (env: CORSA_RPC_PASSWORD)")
	named := flag.Bool("named", false, "Interpret arguments as key=value named parameters")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: corsa-cli [flags] <command> [args...]")
		fmt.Fprintln(os.Stderr, "       corsa-cli help                          — list all commands")
		fmt.Fprintln(os.Stderr, "       corsa-cli <command> arg1 arg2 ...        — positional arguments")
		fmt.Fprintln(os.Stderr, "       corsa-cli <command> key=value ...        — named arguments")
		fmt.Fprintln(os.Stderr, "       corsa-cli <command> '{\"key\": \"value\"}' — JSON arguments")
		fmt.Fprintln(os.Stderr, "       corsa-cli -named <command> key=value ... — explicit key=value mode")
		os.Exit(1)
	}

	if (*username == "") != (*password == "") {
		fmt.Fprintln(os.Stderr, "error: both --username and --password must be set, or neither")
		os.Exit(1)
	}

	command := args[0]
	cmdArgs, err := parseArgs(command, args[1:], *named)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	baseURL := "http://" + net.JoinHostPort(*host, *port)
	result, statusCode, err := execRPC(baseURL, command, cmdArgs, *username, *password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, result, "", "  "); err != nil {
		fmt.Print(string(result))
	} else {
		fmt.Println(prettyJSON.String())
	}

	if statusCode >= 400 {
		os.Exit(1)
	}
}

// envOrDefault returns os.Getenv(key) if it is non-empty, otherwise fallback.
// An empty env value is treated as unset — same semantics as the node-side
// helper in internal/core/config, so CLI flag defaults and node config stay
// symmetric for the shared CORSA_RPC_* variables.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseArgs converts CLI arguments into a named args map.
//
// Four modes:
//  1. No extra args → nil (no args field in request)
//  2. Single arg that looks like JSON object → parse as map
//  3. -named flag → key=value pairs parsed into map
//  4. All args are key=value pairs → parsed into map (auto-detected)
//  5. Otherwise → positional args, delegated to ParseConsoleInput,
//     the shared parser that maps positional args to named fields.
//     This matches what the desktop console and rpc.Client use.
func parseArgs(command string, args []string, named bool) (map[string]interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}

	if named {
		return parseNamedArgs(args)
	}

	// Single argument starting with '{' — treat as raw JSON
	if len(args) == 1 && strings.HasPrefix(strings.TrimSpace(args[0]), "{") {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(args[0]), &m); err != nil {
			return nil, fmt.Errorf("invalid JSON argument: %w", err)
		}
		return m, nil
	}

	// Key=value mode: only when ALL args are valid key=value pairs
	// (each has '=' with a non-empty key before it). If any arg is bare,
	// treat the whole input as positional — body text like "a=b" is common
	// and must not trigger key=value mode.
	allKeyValue := true
	for _, arg := range args {
		idx := strings.IndexByte(arg, '=')
		if idx < 1 {
			allKeyValue = false
			break
		}
	}
	if allKeyValue {
		return parseNamedArgs(args)
	}

	// Bare positional args — reconstruct console input and delegate to
	// ParseConsoleInput, the single source of truth for positional mapping.
	input := command + " " + strings.Join(args, " ")
	req, err := rpc.ParseConsoleInput(input)
	if err != nil {
		return nil, err
	}
	return req.Args, nil
}

// parseNamedArgs converts key=value pairs into a map.
// Values are always stored as strings — the same semantics as the in-process
// console parser (tryParseKeyValue). Handlers that need numeric values use
// numericArg() to accept both string and float64 representations. Typed JSON
// values (numbers, booleans, arrays) are handled by the raw-JSON path above.
func parseNamedArgs(args []string) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(args))
	for _, arg := range args {
		idx := strings.IndexByte(arg, '=')
		if idx < 1 {
			return nil, fmt.Errorf("expected key=value, got %q (use -named or pass JSON)", arg)
		}
		key := arg[:idx]
		value := arg[idx+1:]
		m[key] = value
	}
	return m, nil
}

// execRPC sends a command to POST /rpc/v1/exec and returns raw response.
func execRPC(baseURL, command string, args map[string]interface{}, username, password string) ([]byte, int, error) {
	reqBody := map[string]interface{}{
		"command": command,
	}
	if args != nil {
		reqBody["args"] = args
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("POST", baseURL+"/rpc/v1/exec", bytes.NewReader(data))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, resp.StatusCode, fmt.Errorf("unauthorized: check RPC username/password")
	}

	return body, resp.StatusCode, nil
}
