package config

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/connbudget"
)

// TestBrokenTotalConnectionsSettingIsNotSilentlyOff pins the distinction the
// shared ceiling depends on: unset means "off, deliberately", while a typo
// means "wrong". Collapsing the second into the first would leave a node
// running without the limit its operator asked for, with nothing to find in
// the logs — so the invalid value survives to connbudget.New and stops the
// node there.
func TestBrokenTotalConnectionsSettingIsNotSilentlyOff(t *testing.T) {
	t.Run("unset is a deliberate off", func(t *testing.T) {
		t.Setenv("CORSA_MAX_TOTAL_CONNECTIONS", "")
		if got := maxTotalConnectionsFromEnv(); got != 0 {
			t.Fatalf("unset = %d, want 0", got)
		}
	})

	for name, raw := range map[string]string{
		"not a number": "eight",
		"negative":     "-4",
		"garbage":      "12x",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CORSA_MAX_TOTAL_CONNECTIONS", raw)
			value := maxTotalConnectionsFromEnv()
			if value >= 0 {
				t.Fatalf("%q parsed to %d; a broken setting must not become a valid ceiling (and 0 means OFF)", raw, value)
			}

			node := Node{MaxTotalConnections: value}
			effective := node.EffectiveMaxTotalConnections()
			if effective != value {
				t.Fatalf("EffectiveMaxTotalConnections clamped %d to %d — the error would never reach the budget", value, effective)
			}
			if _, err := connbudget.New(connbudget.Config{Total: effective}); err == nil {
				t.Fatalf("connbudget.New accepted total %d; the node would start unprotected", effective)
			}
		})
	}
}

// TestBrokenAuxiliarySettingIsNotSilentlyDefaulted is the same rule for the
// auxiliary capacity, and review found it broken in a subtler way: the parser
// produced the invalid sentinel correctly, and the effective getter then
// replaced it with the DEFAULT. A typo became a working configuration nobody
// chose — with the shared ceiling on, the node started instead of refusing.
func TestBrokenAuxiliarySettingIsNotSilentlyDefaulted(t *testing.T) {
	for name, raw := range map[string]string{
		"not a number": "four",
		"negative":     "-2",
		"garbage":      "4x",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CORSA_MAX_AUXILIARY_CONNECTIONS", raw)
			value := maxAuxiliaryConnectionsFromEnv()
			if value >= 0 {
				t.Fatalf("%q parsed to %d; a broken setting must not become a valid capacity", raw, value)
			}

			// With the shared ceiling ON — the case where the auxiliary
			// bound actually applies.
			node := Node{MaxTotalConnections: 16, MaxAuxiliaryConnections: value}
			effective := node.EffectiveMaxAuxiliaryConnections()
			if effective >= 0 {
				t.Fatalf("EffectiveMaxAuxiliaryConnections turned %d into %d — the error never reaches the budget",
					value, effective)
			}
			if _, err := connbudget.New(connbudget.Config{
				Total:        node.EffectiveMaxTotalConnections(),
				MaxAuxiliary: effective,
			}); err == nil {
				t.Fatal("connbudget.New accepted a negative auxiliary capacity; the node would start")
			}

			// And with the ceiling OFF: a broken setting is still broken,
			// even where the value would otherwise be unused.
			off := Node{MaxAuxiliaryConnections: value}
			if got := off.EffectiveMaxAuxiliaryConnections(); got >= 0 {
				t.Fatalf("with the ceiling off, EffectiveMaxAuxiliaryConnections = %d, want the invalid sentinel", got)
			}
		})
	}

	t.Run("unset takes the default when the ceiling is on", func(t *testing.T) {
		t.Setenv("CORSA_MAX_AUXILIARY_CONNECTIONS", "")
		node := Node{MaxTotalConnections: 16, MaxAuxiliaryConnections: maxAuxiliaryConnectionsFromEnv()}
		if got := node.EffectiveMaxAuxiliaryConnections(); got != DefaultAuxiliaryConnections {
			t.Fatalf("effective auxiliary = %d, want %d", got, DefaultAuxiliaryConnections)
		}
	})
}
