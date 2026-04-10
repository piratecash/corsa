package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/config"

	"github.com/rs/zerolog/log"
)

// Client is an HTTP client for the RPC server.
// Named commands go through POST /rpc/v1/exec; raw JSON frames go through
// POST /rpc/v1/frame, which dispatches through CommandTable and rejects
// unregistered frame types with 400. Both endpoints are used by corsa-cli
// and external tools.
type Client struct {
	baseURL    string
	httpClient *http.Client
	username   string
	password   string
}

// NewClient creates a new RPC client from configuration.
// Logs a warning if only one of username/password is set — auth will be
// silently skipped, which is almost certainly a misconfiguration.
func NewClient(cfg config.RPC) *Client {
	if (cfg.Username == "") != (cfg.Password == "") {
		log.Warn().Msg("rpc client: partial credentials — both username and password must be set for auth; proceeding without auth")
	}
	return &Client{
		baseURL:    "http://" + net.JoinHostPort(cfg.Host, cfg.Port),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		username:   cfg.Username,
		password:   cfg.Password,
	}
}

// FetchCommands retrieves the list of available commands from the RPC server.
func (c *Client) FetchCommands() ([]CommandInfo, error) {
	result, err := c.exec("help", nil)
	if err != nil {
		return nil, fmt.Errorf("fetch commands: %w", err)
	}

	commandsRaw, ok := result["commands"]
	if !ok {
		return nil, fmt.Errorf("missing 'commands' in help response")
	}

	data, err := json.Marshal(commandsRaw)
	if err != nil {
		return nil, fmt.Errorf("marshal commands: %w", err)
	}

	var commands []CommandInfo
	if err := json.Unmarshal(data, &commands); err != nil {
		return nil, fmt.Errorf("unmarshal commands: %w", err)
	}
	return commands, nil
}

// ExecuteCommand sends a console command string to the RPC server and returns
// the JSON response as a formatted string.
//
// Raw JSON frames (input starting with '{') are sent to POST /rpc/v1/frame,
// which dispatches through CommandTable. Registered commands may normalize
// or rebuild frame fields; unregistered frame types are rejected with 400.
// Named commands are parsed by ParseConsoleInput into {command, args}
// and dispatched through POST /rpc/v1/exec.
func (c *Client) ExecuteCommand(input string) (string, error) {
	trimmed := strings.TrimSpace(input)

	var raw []byte
	var statusCode int
	var err error

	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		// Raw JSON frame → dispatch through /frame endpoint (CommandTable).
		var frameBody map[string]interface{}
		if jsonErr := json.Unmarshal([]byte(trimmed), &frameBody); jsonErr != nil {
			return "", fmt.Errorf("invalid JSON frame: %w", jsonErr)
		}
		raw, statusCode, err = c.postRaw("/rpc/v1/frame", frameBody)
	} else {
		req, parseErr := ParseConsoleInput(input)
		if parseErr != nil {
			return "", parseErr
		}
		reqBody := map[string]interface{}{
			"command": req.Name,
		}
		if req.Args != nil {
			reqBody["args"] = req.Args
		}
		raw, statusCode, err = c.postRaw("/rpc/v1/exec", reqBody)
	}
	if err != nil {
		return "", err
	}

	if statusCode >= 400 {
		var errResp map[string]interface{}
		if jsonErr := json.Unmarshal(raw, &errResp); jsonErr == nil {
			if errMsg, ok := errResp["error"].(string); ok {
				return "", fmt.Errorf("rpc error: %s", errMsg)
			}
		}
		return "", fmt.Errorf("rpc error: status %d", statusCode)
	}

	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, raw, "", "  "); err != nil {
		return string(raw), nil
	}
	return prettyJSON.String(), nil
}

// exec sends a command via /rpc/v1/exec and returns parsed JSON object.
func (c *Client) exec(command string, args map[string]interface{}) (map[string]interface{}, error) {
	reqBody := map[string]interface{}{
		"command": command,
	}
	if args != nil {
		reqBody["args"] = args
	}

	raw, statusCode, err := c.postRaw("/rpc/v1/exec", reqBody)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if statusCode >= 400 {
		if errMsg, ok := result["error"].(string); ok {
			return nil, fmt.Errorf("rpc error: %s", errMsg)
		}
		return nil, fmt.Errorf("rpc error: status %d", statusCode)
	}

	return result, nil
}

// postRaw sends a POST request and returns the raw response body and status code.
func (c *Client) postRaw(path string, body map[string]interface{}) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	} else {
		bodyReader = bytes.NewReader([]byte("{}"))
	}

	req, err := http.NewRequest("POST", c.baseURL+path, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if c.username != "" && c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("rpc request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, resp.StatusCode, fmt.Errorf("unauthorized: check RPC username/password")
	}

	return respBody, resp.StatusCode, nil
}
