package node

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// resource_breakdown_test.go pins the breakdown at the surface an operator
// reads, and pins the one property that makes it worth reading: every
// subsystem that can hold state is present, so a growth this node suffers has
// a line it can appear on.
//
// Reference: docs/refactoring/dht/13-measurements.md §5.

// TestBreakdownCoversEveryStateOwningSubsystem is the coverage assertion.
//
// A breakdown that silently omitted a subsystem would answer "nobody is
// holding it" for exactly the growth it exists to find — which is worse than
// having no breakdown at all, because it looks like an answer. The ban domain
// is the reason this is asserted rather than trusted: a leak once lived there
// for several releases while every diagnostic the node had reported nothing.
func TestBreakdownCoversEveryStateOwningSubsystem(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	breakdown := svc.ResourceBreakdown()

	present := make(map[domain.ResourceSubsystem]bool)
	for _, usage := range breakdown.Subsystems() {
		if !usage.Subsystem().Valid() {
			t.Fatalf("a subsystem reported itself as %v", usage.Subsystem())
		}
		if present[usage.Subsystem()] {
			t.Fatalf("%s appears twice: two lines for one owner disagree the moment either changes",
				usage.Subsystem())
		}
		present[usage.Subsystem()] = true
	}

	for _, want := range []domain.ResourceSubsystem{
		domain.ResourceSubsystemRoutePlane,
		domain.ResourceSubsystemAnnounce,
		domain.ResourceSubsystemDatagram,
		domain.ResourceSubsystemDelivery,
		domain.ResourceSubsystemSessions,
		domain.ResourceSubsystemKnowledge,
		domain.ResourceSubsystemBans,
	} {
		if !present[want] {
			t.Fatalf("%s is missing from the breakdown: growth there would read as nobody's", want)
		}
	}

	if breakdown.SampledAt().IsZero() {
		t.Fatal("the breakdown carries no sampling time")
	}
	if time.Since(breakdown.SampledAt()) > time.Minute {
		t.Fatalf("sampled_at is %v old on a freshly taken sample", time.Since(breakdown.SampledAt()))
	}
}

// TestBreakdownCountsRealState checks the numbers move with the node rather
// than being a shape full of zeroes: a live peer must appear in the sessions
// line, and its floor must follow.
//
// The mutation this kills: an assembler wired to the wrong map, or to a map
// that is always empty — which a structural test alone would pass.
func TestBreakdownCountsRealState(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	before := sessionGauge(t, svc.ResourceBreakdown())

	peer := domain.PeerIdentityFromWire(strings.Repeat("5", 40))
	installDatagramPeer(t, svc, peer, datagramPeerConn{
		version:     domain.ProtocolVersion(config.ProtocolVersion),
		connectedAt: time.Now().UTC().Add(-time.Hour),
	})

	after := sessionGauge(t, svc.ResourceBreakdown())
	if after.Count() != before.Count()+1 {
		t.Fatalf("sessions gauge went from %d to %d after adding one peer", before.Count(), after.Count())
	}
	if after.FloorBytes() <= before.FloorBytes() {
		t.Fatalf("the sessions floor did not grow with the session count (%d → %d)",
			before.FloorBytes(), after.FloorBytes())
	}
}

// sessionGauge extracts the sessions gauge, failing loudly rather than
// returning a zero value that a comparison would silently pass.
func sessionGauge(t *testing.T, breakdown domain.ResourceBreakdown) domain.ResourceGauge {
	t.Helper()
	for _, usage := range breakdown.Subsystems() {
		if usage.Subsystem() != domain.ResourceSubsystemSessions {
			continue
		}
		for _, gauge := range usage.Gauges() {
			if gauge.Name() == "sessions" {
				return gauge
			}
		}
	}
	t.Fatal("the sessions subsystem reports no sessions gauge")
	return domain.ResourceGauge{}
}

// TestFetchResourceBreakdownRendersFloorsAndDominant covers the wire shape an
// operator and a dashboard actually consume.
func TestFetchResourceBreakdownRendersFloorsAndDominant(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	peer := domain.PeerIdentityFromWire(strings.Repeat("6", 40))
	installDatagramPeer(t, svc, peer, datagramPeerConn{
		version:     domain.ProtocolVersion(config.ProtocolVersion),
		connectedAt: time.Now().UTC().Add(-time.Hour),
	})

	raw, err := svc.FetchResourceBreakdown()
	if err != nil {
		t.Fatalf("FetchResourceBreakdown: %v", err)
	}

	var rendered struct {
		SampledAt  string `json:"sampled_at"`
		Dominant   string `json:"dominant"`
		FloorBytes uint64 `json:"floor_bytes"`
		FloorHuman string `json:"floor_human"`
		Subsystems []struct {
			Subsystem  string `json:"subsystem"`
			FloorBytes uint64 `json:"floor_bytes"`
			FloorHuman string `json:"floor_human"`
			Gauges     []struct {
				Name       string `json:"name"`
				Count      uint64 `json:"count"`
				EntryBytes uint64 `json:"entry_bytes"`
				FloorBytes uint64 `json:"floor_bytes"`
			} `json:"gauges"`
		} `json:"subsystems"`
	}
	if err := json.Unmarshal(raw, &rendered); err != nil {
		t.Fatalf("unmarshal breakdown: %v", err)
	}

	if rendered.SampledAt == "" || rendered.FloorHuman == "" {
		t.Fatalf("the breakdown omits its own header: %s", raw)
	}
	if rendered.FloorBytes == 0 {
		t.Fatalf("a node holding a live session reported a zero floor: %s", raw)
	}
	// Named because there IS one: the node holds state, so refusing to name a
	// dominant consumer would be the empty-node answer given on a loaded node.
	if rendered.Dominant == "" {
		t.Fatalf("no dominant subsystem named on a node holding %d bytes", rendered.FloorBytes)
	}

	total := uint64(0)
	for _, subsystem := range rendered.Subsystems {
		gaugeTotal := uint64(0)
		for _, gauge := range subsystem.Gauges {
			if gauge.FloorBytes != gauge.Count*gauge.EntryBytes {
				t.Fatalf("%s/%s: floor %d does not equal count %d × entry %d",
					subsystem.Subsystem, gauge.Name, gauge.FloorBytes, gauge.Count, gauge.EntryBytes)
			}
			gaugeTotal += gauge.FloorBytes
		}
		if subsystem.FloorBytes != gaugeTotal {
			t.Fatalf("%s: floor %d does not equal the sum of its gauges %d",
				subsystem.Subsystem, subsystem.FloorBytes, gaugeTotal)
		}
		total += subsystem.FloorBytes
	}
	if rendered.FloorBytes != total {
		t.Fatalf("breakdown floor %d does not equal the sum of its subsystems %d",
			rendered.FloorBytes, total)
	}
}

// TestBreakdownAnswersOnANodeWithoutTheDatagramPlane keeps the diagnostic
// usable on the deployment that has least to spare.
//
// A node built without the plane is a real configuration, not a failure, and a
// breakdown that refused to answer there would be missing on exactly the nodes
// an operator reaches for it on.
func TestBreakdownAnswersOnANodeWithoutTheDatagramPlane(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, false)
	if svc.datagramLayer() != nil {
		t.Fatal("the fixture built a plane the test needs absent")
	}

	if _, err := svc.FetchResourceBreakdown(); err != nil {
		t.Fatalf("FetchResourceBreakdown refused on a node without the plane: %v", err)
	}
	for _, usage := range svc.ResourceBreakdown().Subsystems() {
		if usage.Subsystem() != domain.ResourceSubsystemDatagram {
			continue
		}
		if len(usage.Gauges()) != 0 {
			t.Fatalf("an absent plane reported %d gauges", len(usage.Gauges()))
		}
		return
	}
	t.Fatal("the datagram subsystem vanished instead of reporting empty: an absent plane is a state, not a missing line")
}
