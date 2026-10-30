//go:build s6_observation_probe && !d1_resource_probe

package node

import (
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

func appS6ProbeRequest() S6ObservationProbeRequest {
	return S6ObservationProbeRequest{
		RegisteredProvenV2:    appRegisteredRequest(),
		AdvertisedConnections: appAdvertisedRequest(),
		RawPinMembership:      appRawPinRequest(),
		Guard:                 appGuardKey(), Terminal: appTerminalKey(),
		Pending:         corenode.PendingObservationKey{Anchor: "s6-probe-pending", ArtifactSHA256: appHandshakeKey().ArtifactSHA256, Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-duration"},
		Handshake:       corenode.HandshakeObservationRequest{Key: appHandshakeKey(), Capacity: 8},
		ConnectionTotal: corenode.ConnectionTotalObservationRequest{Key: appConnectionTotalKey(), Capacity: 8},
	}
}

func assertAppS6ProbeHandles(t *testing.T, a *App, want S6ObservationProbeRequest) {
	t.Helper()
	if a == nil || a.service == nil {
		t.Fatal("probe did not return its original App/Service")
	}
	r, ad, raw := a.RegisteredProvenV2Observation(), a.AdvertisedConnectionObservation(), a.RawPinMembershipObservation()
	g, tv, p := a.GuardObservation(), a.TerminalObservation(), a.PendingObservation()
	h, total := a.HandshakeObservation(), a.ConnectionTotalObservation()
	if r == nil || ad == nil || raw == nil || g == nil || tv == nil || p == nil || h == nil || total == nil {
		t.Fatal("probe lost one of its eight requested handles")
	}
	if r != a.registeredProvenV2Observation || ad != a.advertisedConnectionObservation || raw != a.rawPinMembershipObservation || g != a.guardObservation || tv != a.terminalObservation || p != a.pendingObservation || h != a.handshakeObservation || total != a.connectionTotalObservation {
		t.Fatal("probe fabricated a handle outside its original App")
	}
	owner := domain.PeerIdentityFromWire(a.service.Address())
	if owner.IsZero() || r.Identity() != owner || ad.Identity() != owner || raw.Identity() != owner || g.Identity() != owner || tv.Identity() != owner || p.Identity() != owner || h.Identity() != owner || total.Identity() != owner {
		t.Fatal("probe handles do not belong to the one original App owner")
	}
	if r.Key().Instance != want.RegisteredProvenV2.Instance || ad.Key().Instance != want.AdvertisedConnections.Instance || raw.Key().Instance != want.RawPinMembership.Instance || g.Key() != want.Guard || tv.Key() != want.Terminal || p.Key() != want.Pending || h.Key() != want.Handshake.Key || total.Key() != want.ConnectionTotal.Key {
		t.Fatal("probe omitted, swapped or retained mutable request keys")
	}
	if a.HandshakeFileObservation() != nil || a.ConnectionTotalFileObservation() != nil || a.service.StartupRefusal() != nil {
		t.Fatal("memory-only probe installed file resources or changed startup policy")
	}
}

func TestAppS6ObservationProbeConstruction(t *testing.T) {
	isolateTotalApp(t)
	request := appS6ProbeRequest()
	want := request
	a, err := NewWithS6ObservationProbe(request)
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, time.Now().Add(10*time.Second))
	assertAppS6ProbeHandles(t, a, want)
	request = S6ObservationProbeRequest{}
	if request == want {
		t.Fatal("fixture did not change caller request")
	}
	// Returned keys are also values; a caller cannot change the installed key.
	key := a.HandshakeObservation().Key()
	key.Epoch = [16]byte{}
	if key == a.HandshakeObservation().Key() {
		t.Fatal("fixture did not change returned key")
	}
	assertAppS6ProbeHandles(t, a, want)
	r, ad, raw := a.RegisteredProvenV2Observation().Snapshot(), a.AdvertisedConnectionObservation().Snapshot(), a.RawPinMembershipObservation().Snapshot()
	appRegisteredConservative(t, r)
	appAdvertisedLabels(t, ad)
	appRawPinLabels(t, raw)
	if r.Instance != want.RegisteredProvenV2.Instance || ad.Instance != want.AdvertisedConnections.Instance || raw.Instance != want.RawPinMembership.Instance || r.Sequence != 0 || ad.CompletedSequence != 0 || ad.CoveredSequence != 0 || raw.Sequence != 0 {
		t.Fatal("probe substituted a K3 source or baseline", r, ad, raw)
	}
	p, tv, g := a.PendingObservation().Snapshot(), a.TerminalObservation().Snapshot(), a.GuardObservation().Snapshot()
	h, total := a.HandshakeObservation().Snapshot(), a.ConnectionTotalObservation().Snapshot()
	if p.AttributionBound || tv.AttributionBound || g.AttributionBound || h.AttributionBound || total.AttributionBound || !tv.Available || !g.Available || !h.Available || tv.Registered != 0 || g.Instance != want.Guard.ClockInstance || h.Source.Epoch != want.Handshake.Key.Epoch {
		t.Fatal("probe changed source availability, attribution or empty owner baseline", p, tv, g, h, total)
	}
	if !total.Available || total.Status != "DISABLED" || total.Reason != "total-disabled" || total.Config.Total != 0 || total.Cut.Available || total.Rows != nil || !total.PastOnly {
		t.Fatal("probe activated B0 or fabricated measured zero", total)
	}
	if _, clockErr := trustedclock.SinceBoot(); clockErr == nil {
		t.Run("actual-pending-owner-commit", func(t *testing.T) {
			result, err := a.service.CancelOutgoingDelivery(protocol.MessageID("s6-probe-empty-cancel"), domain.PeerIdentityFromWire(strings.Repeat("2", 40)))
			if err != nil || result.Total() != 0 {
				t.Fatal("original empty cancellation changed", result, err)
			}
			after := a.PendingObservation().Snapshot()
			direct := a.service.PendingQueueObservation()
			if !after.CoverageValid || after.AttributionBound || after.SourceID != p.SourceID || after.Sequence != p.Sequence+1 || after.Count != 0 || direct.SourceID != after.SourceID || direct.Sequence != after.Sequence {
				t.Fatal("probe readout missed its original Service mutation", p, after, direct)
			}
		})
	} else if p.CoverageValid || p.UnknownReason == "" || g.Status != "UNKNOWN" || h.Status != "UNKNOWN" {
		t.Fatal("unavailable platform clock became covered evidence", p, g, h)
	}
	t.Run("equal-labels-do-not-collapse-App-owners", func(t *testing.T) {
		isolateTotalApp(t)
		other, err := NewWithS6ObservationProbe(want)
		if err != nil {
			t.Fatal(err)
		}
		ownUnstartedGuardApp(t, other, time.Now().Add(10*time.Second))
		assertAppS6ProbeHandles(t, other, want)
		if other.service == a.service || other.AdvertisedConnectionObservation() == a.AdvertisedConnectionObservation() || other.AdvertisedConnectionObservation().Identity() == a.AdvertisedConnectionObservation().Identity() || p.Available && other.PendingObservation().Snapshot().SourceID == p.SourceID || other.TerminalObservation().Snapshot().Instance == tv.Instance {
			t.Fatal("equal caller labels collapsed distinct original App/source owners")
		}
	})
}

func TestAppS6ObservationProbeInvalidSourceRequests(t *testing.T) {
	for _, name := range []string{"registered", "advertised", "raw-pin", "handshake", "disabled-total"} {
		t.Run(name, func(t *testing.T) {
			isolateTotalApp(t)
			request := appS6ProbeRequest()
			switch name {
			case "registered":
				request.RegisteredProvenV2.OwnerCapacity = 0
			case "advertised":
				request.AdvertisedConnections.SlotCapacity = 0
			case "raw-pin":
				request.RawPinMembership.BaselineCapacity = 0
			case "handshake":
				request.Handshake.Capacity = 0
			case "disabled-total":
				request.ConnectionTotal.Capacity = 0
				request.ConnectionTotal.Key.Epoch = [16]byte{}
			}
			a, err := NewWithS6ObservationProbe(request)
			if err != nil {
				t.Fatal("observer invalidity became App startup refusal", err)
			}
			ownUnstartedGuardApp(t, a, time.Now().Add(10*time.Second))
			assertAppS6ProbeHandles(t, a, request)
			status, reason, attributed := "", "", false
			switch name {
			case "registered":
				v := a.RegisteredProvenV2Observation().Snapshot()
				appRegisteredConservative(t, v)
				status, reason, attributed = v.Status, v.Reason, v.AttributionBound
			case "advertised":
				v := a.AdvertisedConnectionObservation().Snapshot()
				appAdvertisedLabels(t, v)
				status, reason, attributed = v.Status, v.Reason, v.AttributionBound
			case "raw-pin":
				v := a.RawPinMembershipObservation().Snapshot()
				appRawPinLabels(t, v)
				status, reason, attributed = v.Status, v.Reason, v.AttributionBound
			case "handshake":
				v := a.HandshakeObservation().Snapshot()
				status, reason, attributed = v.Status, v.Reason, v.AttributionBound
				if !v.PastOnly || v.Cut.Available {
					t.Fatal("invalid handshake fabricated a covered cut", v)
				}
			case "disabled-total":
				v := a.ConnectionTotalObservation().Snapshot()
				if v.Status != "DISABLED" || v.Reason != "total-disabled" || v.Config.Total != 0 || v.Cut.Available || v.Rows != nil || v.AttributionBound || !v.PastOnly {
					t.Fatal("B0 no longer precedes capacity/source-token validation", v)
				}
				return
			}
			if status != "UNKNOWN" || reason == "" || attributed {
				t.Fatal("probe normalized invalid-source uncertainty", status, reason, attributed)
			}
		})
	}
}

func TestAppS6ObservationProbeErrorsAndDefaults(t *testing.T) {
	t.Run("ordinary-App-all-eight-OFF", func(t *testing.T) {
		isolateTotalApp(t)
		a := New()
		ownUnstartedGuardApp(t, a, time.Now().Add(10*time.Second))
		if a.RegisteredProvenV2Observation() != nil || a.AdvertisedConnectionObservation() != nil || a.RawPinMembershipObservation() != nil || a.GuardObservation() != nil || a.TerminalObservation() != nil || a.PendingObservation() != nil || a.HandshakeObservation() != nil || a.ConnectionTotalObservation() != nil || a.HandshakeFileObservation() != nil || a.ConnectionTotalFileObservation() != nil {
			t.Fatal("diagnostic build changed ordinary App defaults")
		}
	})
	for _, name := range []string{"pending", "terminal", "guard", "handshake", "total"} {
		t.Run("invalid-key-"+name, func(t *testing.T) {
			isolateTotalApp(t)
			request := appS6ProbeRequest()
			switch name {
			case "pending":
				request.Pending = corenode.PendingObservationKey{}
			case "terminal":
				request.Terminal = corenode.TerminalObservationKey{}
			case "guard":
				request.Guard = corenode.GuardObservationKey{}
			case "handshake":
				request.Handshake.Key = corenode.HandshakeObservationKey{}
			case "total":
				request.ConnectionTotal.Key = corenode.ConnectionTotalObservationKey{}
			}
			if a, err := NewWithS6ObservationProbe(request); err == nil || a != nil {
				t.Fatal("probe swallowed original constructor key error", a, err)
			}
		})
	}
	t.Run("original-identity-I/O-error", func(t *testing.T) {
		isolateTotalApp(t)
		// Reading a directory as the identity file fails before construction.
		t.Setenv("CORSA_IDENTITY_PATH", t.TempDir())
		if a, err := NewWithS6ObservationProbe(appS6ProbeRequest()); err == nil || a != nil {
			t.Fatal("probe swallowed original identity load error", a, err)
		}
	})
}

func TestAppS6ObservationProbeUnstartedLifecycle(t *testing.T) {
	isolateTotalApp(t)
	request := appS6ProbeRequest()
	a, err := NewWithS6ObservationProbe(request)
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, time.Now().Add(10*time.Second))
	assertAppS6ProbeHandles(t, a, request)
	r, ad, raw := a.RegisteredProvenV2Observation(), a.AdvertisedConnectionObservation(), a.RawPinMembershipObservation()
	p, tv, g := a.PendingObservation(), a.TerminalObservation(), a.GuardObservation()
	h, total := a.HandshakeObservation(), a.ConnectionTotalObservation()
	// Original memory-only construction owns no file observation resources.
	// Exercise the existing idempotent cleanup without starting App.Run.
	for i := 0; i < 2; i++ {
		if err := a.CloseHandshakeFileObservation(); err != nil {
			t.Fatal(err)
		}
		if err := a.CloseConnectionTotalFileObservation(); err != nil {
			t.Fatal(err)
		}
	}
	assertAppS6ProbeHandles(t, a, request)
	if r != a.RegisteredProvenV2Observation() || ad != a.AdvertisedConnectionObservation() || raw != a.RawPinMembershipObservation() || p != a.PendingObservation() || tv != a.TerminalObservation() || g != a.GuardObservation() || h != a.HandshakeObservation() || total != a.ConnectionTotalObservation() {
		t.Fatal("original cleanup replaced a memory source")
	}
}
