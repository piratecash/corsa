package main

import "testing"

func TestCandidateSupervisionRequiresExplicitCompleteProfile(t *testing.T) {
	for _, test := range []struct {
		name               string
		environment        map[string]string
		enabled, wantError bool
	}{
		{name: "ordinary"},
		{name: "fd-alone", environment: map[string]string{"CORSA_STARTUP_FENCE_FD": "3"}, wantError: true},
		{name: "candidate-alone", environment: map[string]string{"CORSA_STARTUP_FENCE_CANDIDATE": "1"}, wantError: true},
		{name: "wrong-fd", environment: map[string]string{"CORSA_STARTUP_FENCE_CANDIDATE": "1", "CORSA_STARTUP_FENCE_FD": "4", "CORSA_STARTUP_FENCE_LAUNCHER": "/opt/corsa-supervisor", "CORSA_STARTUP_FENCE_STORE": "store"}, wantError: true},
		{name: "relative-launcher", environment: map[string]string{"CORSA_STARTUP_FENCE_CANDIDATE": "1", "CORSA_STARTUP_FENCE_FD": "3", "CORSA_STARTUP_FENCE_LAUNCHER": "supervisor", "CORSA_STARTUP_FENCE_STORE": "store"}, wantError: true},
		{name: "explicit", environment: map[string]string{"CORSA_STARTUP_FENCE_CANDIDATE": "1", "CORSA_STARTUP_FENCE_FD": "3", "CORSA_STARTUP_FENCE_LAUNCHER": "/opt/corsa-supervisor", "CORSA_STARTUP_FENCE_STORE": "store"}, enabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := candidateSupervisionFromEnvironment(func(name string) string { return test.environment[name] })
			if (err != nil) != test.wantError || cfg.enabled != test.enabled {
				t.Fatalf("configuration enabled=%v err=%v", cfg.enabled, err)
			}
		})
	}
}
