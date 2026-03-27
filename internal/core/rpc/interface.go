package rpc

// CommandInfo provides metadata about an RPC command.
type CommandInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Usage       string `json:"usage,omitempty"`
}
