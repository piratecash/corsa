package node

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

func isolatePendingApp(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for env, name := range map[string]string{"CORSA_IDENTITY_PATH": "identity.json", "CORSA_TRUST_STORE_PATH": "trust.json",
		"CORSA_PEERS_PATH": "peers.json", "CORSA_IDENTITY_INTENTS_PATH": "intents.json", "CORSA_SECURE_SESSION_STORE_PATH": "secure.json"} {
		t.Setenv(env, filepath.Join(dir, name))
	}
	t.Setenv("CORSA_CHATLOG_DIR", dir)
	t.Setenv("CORSA_LISTEN_ADDRESS", "127.0.0.1:64646")
	t.Setenv("CORSA_BOOTSTRAP_PEERS", "127.0.0.1:64647")
}

func TestAppPendingObservationActualConstruction(t *testing.T) {
	isolatePendingApp(t)
	key := corenode.PendingObservationKey{Anchor: "app-anchor", ArtifactSHA256: strings.Repeat("b", 64),
		Boot: "app-boot", ProcessInstance: "app-instance", ClockDomain: "boot-duration-app"}
	a, err := NewWithPendingObservation(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.service.WaitBackground)
	o := a.PendingObservation()
	if o == nil || o.Key() != key || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) {
		t.Fatal("App did not retain the actual construction handle")
	}
	key.Anchor = "changed"
	copyKey := o.Key()
	copyKey.Boot = "changed"
	if o.Key().Anchor != "app-anchor" || o.Key().Boot != "app-boot" {
		t.Fatal("App retained caller-mutable attribution")
	}
	before := o.Snapshot()
	if _, clockErr := trustedclock.SinceBoot(); clockErr != nil {
		if before.Available || before.CoverageValid || before.UnknownReason == "" {
			t.Fatalf("unsupported platform claimed observation: %+v", before)
		}
		return
	}
	if !before.Available || !before.CoverageValid || before.Sequence != 0 || before.Count != 0 || before.AttributionBound {
		t.Fatalf("App source missing before first operation: %+v", before)
	}
	t.Run("actual-owner", func(t *testing.T) {
		// This is the actual public queue-cancellation owner, even for an empty
		// queue: its committed zero delta must still advance this same source.
		// Positive admissions/ring replacement/removal are tested in core node.
		result, err := a.service.CancelOutgoingDelivery(protocol.MessageID("app-empty-cancel"), domain.PeerIdentityFromWire(strings.Repeat("2", 40)))
		if err != nil || result.Total() != 0 {
			t.Fatalf("actual empty cancellation = %+v, %v", result, err)
		}
		after := a.PendingObservation().Snapshot()
		if !after.CoverageValid || after.SourceID != before.SourceID || after.Sequence != before.Sequence+1 || after.Count != 0 || after.AttributionBound {
			t.Fatalf("App readout missed its actual owner commit: %+v -> %+v", before, after)
		}
		if direct := a.service.PendingQueueObservation(); direct.SourceID != after.SourceID || direct.Sequence != after.Sequence {
			t.Fatal("App readout is not its own Service source")
		}
		if err := a.service.StartupRefusal(); err != nil {
			t.Fatalf("observation changed startup: %v", err)
		}
	})
}

func TestAppPendingObservationDefaultAndInvalidKey(t *testing.T) {
	isolatePendingApp(t)
	a := New()
	t.Cleanup(a.service.WaitBackground)
	if a.PendingObservation() != nil || a.service.PendingQueueObservation().Available {
		t.Fatal("ordinary App construction opted in")
	}
	if got, err := NewWithPendingObservation(corenode.PendingObservationKey{}); err == nil || got != nil {
		t.Fatal("invalid App key accepted")
	}
	var absent *App
	if absent.PendingObservation() != nil {
		t.Fatal("nil App fabricated a source")
	}
}
