package rpc

// CommandInfo provides metadata about an RPC command.
type CommandInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Usage       string `json:"usage,omitempty"`
	// Transport declares where the command may be called from. The zero
	// value (TransportAnyAuthenticated) preserves the historical reach, so
	// a command that says nothing keeps saying nothing; restrictions are
	// opt-in and visible at the registration site.
	Transport TransportPolicy `json:"transport,omitempty"`
}
