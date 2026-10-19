package startupfence

import (
	"sort"
	"strings"
)

// This candidate subset is NOT equivalent to every config.Default knob. The
// complete deployed profile needs review before production enablement.
var nodeEnvironmentNames = map[string]struct{}{
	"CORSA_DATA_DIR": {}, "CORSA_LISTEN_ADDRESS": {}, "CORSA_IDENTITY_PATH": {},
	"CORSA_TRUST_STORE_PATH": {}, "CORSA_IDENTITY_INTENTS_PATH": {}, "CORSA_PEERS_PATH": {},
	"CORSA_SECURE_SESSION_STORE_PATH": {}, "CORSA_CHATLOG_DIR": {}, "CORSA_STATE_DB_PATH": {},
	"CORSA_DOWNLOAD_DIR": {}, "CORSA_NODE_TYPE": {}, "CORSA_LISTENER": {}, "CORSA_ADVERTISE_PORT": {},
	"CORSA_BOOTSTRAP_PEERS": {}, "CORSA_BOOTSTRAP_PEER": {}, "CORSA_CONNECT_ONLY": {}, "CORSA_PROXY": {},
	"CORSA_RPC_HOST": {}, "CORSA_RPC_PORT": {}, "CORSA_RPC_USERNAME": {}, "CORSA_RPC_PASSWORD": {},
	"CORSA_V2_TLS_FIRST":            {},
	"CORSA_STARTUP_FENCE_CANDIDATE": {}, "CORSA_STARTUP_FENCE_LAUNCHER": {}, "CORSA_STARTUP_FENCE_STORE": {},
}

func childEnvironment(command ChildCommand) ([]string, error) {
	keys := make([]string, 0, len(command.Environment))
	for name, value := range command.Environment {
		if _, ok := nodeEnvironmentNames[name]; !ok || strings.ContainsRune(string(value), 0) {
			return nil, ErrInvalid
		}
		keys = append(keys, name)
	}
	sort.Strings(keys)
	result := []string{"PATH=/usr/bin:/bin", "CORSA_STARTUP_FENCE_FD=3"}
	for _, key := range keys {
		result = append(result, key+"="+string(command.Environment[key]))
	}
	return result, nil
}
