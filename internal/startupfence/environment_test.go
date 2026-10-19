package startupfence

import (
	"fmt"
	"strings"
	"testing"
)

func TestExplicitNodeEnvironmentCannotOverrideFenceOrIncludeBrokerKeys(t *testing.T) {
	for _, name := range []string{"CORSA_STARTUP_FENCE_FD", "BROKER_PRIVATE_KEY", "PATH"} {
		if _, err := childEnvironment(ChildCommand{Environment: map[string]EnvironmentValue{name: "forbidden"}}); err == nil {
			t.Fatalf("admitted %s", name)
		}
	}
	command := ChildCommand{Environment: map[string]EnvironmentValue{"CORSA_RPC_PASSWORD": "node-secret", "CORSA_DATA_DIR": "/var/lib/corsa"}}
	env, err := childEnvironment(command)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(env, "\n"), "CORSA_RPC_PASSWORD=node-secret") {
		t.Fatal("explicit node configuration lost")
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%d"} {
		if strings.Contains(fmt.Sprintf(verb, command), "node-secret") {
			t.Fatalf("command %s leaked password", verb)
		}
	}
}
