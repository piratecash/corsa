package node

import "os"

// Preserve ordinary Windows startup. The strict nonblocking protected backend
// is unavailable there, so these bytes can never earn confirmed K authority.
func readInitialTrustFile(path string) ([]byte, error) { return os.ReadFile(path) }
