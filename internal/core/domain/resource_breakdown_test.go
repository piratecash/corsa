package domain

import (
	"testing"
	"time"
)

// resource_breakdown_test.go pins the two properties every number derived from
// this type depends on: a floor is a floor, and the dominant consumer is named
// only when there is one.

// TestFloorBytesIsCountTimesEntry keeps the arithmetic honest at every level of
// the sum — one gauge, one subsystem, the whole breakdown — because a budget is
// read at whichever level the reader stopped at, and a total that disagreed
// with its parts would be discovered by nobody.
func TestFloorBytesIsCountTimesEntry(t *testing.T) {
	t.Parallel()

	claims := NewResourceGauge("route_claims", 1_000, 184)
	health := NewResourceGauge("route_health", 2_000, 272)
	if got := claims.FloorBytes(); got != 184_000 {
		t.Fatalf("gauge floor = %d, want %d", got, 184_000)
	}

	plane := NewSubsystemUsage(ResourceSubsystemRoutePlane, claims, health)
	if got := plane.FloorBytes(); got != 184_000+544_000 {
		t.Fatalf("subsystem floor = %d, want the sum of its gauges", got)
	}

	sessions := NewSubsystemUsage(ResourceSubsystemSessions, NewResourceGauge("sessions", 8, 1_000))
	breakdown := NewResourceBreakdown(time.Now().UTC(), plane, sessions)
	if got := breakdown.FloorBytes(); got != 184_000+544_000+8_000 {
		t.Fatalf("breakdown floor = %d, want the sum of its subsystems", got)
	}
}

// TestSaturationGaugeAddsNoBytes pins the fix for a double count that was both
// invisible and easy to repeat.
//
// A saturation gauge counts occupancy of a quota, and the entries it counts
// are by construction a SUBSET of entries the container they live in has
// already reported. Both numbers were individually right; their sum was not,
// and it silently pushed a "floor" above the truth it claims to sit under.
func TestSaturationGaugeAddsNoBytes(t *testing.T) {
	t.Parallel()

	records := NewResourceGauge("reverse_records", 100, 200)
	occupied := NewSaturationGauge("reverse_local_slots", 40)

	if occupied.Count() != 40 {
		t.Fatalf("a saturation gauge must still report its count, got %d", occupied.Count())
	}
	if occupied.FloorBytes() != 0 {
		t.Fatalf("a saturation gauge charged %d bytes: those records are already counted by the container they live in",
			occupied.FloorBytes())
	}
	if occupied.Kind() != ResourceGaugeSaturation || records.Kind() != ResourceGaugeMemory {
		t.Fatal("gauge kinds are the only thing telling a consumer which numbers may be summed")
	}

	usage := NewSubsystemUsage(ResourceSubsystemDatagram, records, occupied)
	if got := usage.FloorBytes(); got != 20_000 {
		t.Fatalf("subsystem floor = %d, want only the memory gauge's %d", got, 20_000)
	}
}

// TestNegativeCountDoesNotBecomeAnExabyte guards the one arithmetic accident
// this type can produce. len never returns a negative, but a caller computing
// a difference can, and an unsigned conversion would turn it into a figure an
// operator would read as a catastrophic leak.
func TestNegativeCountDoesNotBecomeAnExabyte(t *testing.T) {
	t.Parallel()

	gauge := NewResourceGauge("impossible", -1, 4096)
	if gauge.Count() != 0 || gauge.FloorBytes() != 0 {
		t.Fatalf("negative count became %d entries / %d bytes", gauge.Count(), gauge.FloorBytes())
	}
}

// TestDominantNamesNobodyOnAnEmptyNode is the honesty half.
//
// A node that has just started holds nothing, and every subsystem is equally
// the largest. Naming one of them would be an answer with no content — and the
// step this serves asks specifically for the dominant consumer to be named
// "with a number, not a hypothesis".
func TestDominantNamesNobodyOnAnEmptyNode(t *testing.T) {
	t.Parallel()

	empty := NewResourceBreakdown(time.Now().UTC(),
		NewSubsystemUsage(ResourceSubsystemRoutePlane, NewResourceGauge("route_claims", 0, 184)),
		NewSubsystemUsage(ResourceSubsystemDelivery),
	)
	if usage, named := empty.Dominant(); named {
		t.Fatalf("an empty node named %s as its dominant consumer", usage.Subsystem())
	}

	loaded := NewResourceBreakdown(time.Now().UTC(),
		NewSubsystemUsage(ResourceSubsystemSessions, NewResourceGauge("sessions", 8, 1_000)),
		NewSubsystemUsage(ResourceSubsystemRoutePlane, NewResourceGauge("route_claims", 1_000, 184)),
	)
	usage, named := loaded.Dominant()
	if !named || usage.Subsystem() != ResourceSubsystemRoutePlane {
		t.Fatalf("dominant = %v/%v, want the route plane", usage.Subsystem(), named)
	}
}

// TestGaugesAndSubsystemsAreCopies keeps a rendered diagnostic from being
// rewritten under a reader. The slices come from live containers, and a
// consumer holding one while an assembler reuses its backing array would see a
// breakdown change as it printed.
func TestGaugesAndSubsystemsAreCopies(t *testing.T) {
	t.Parallel()

	gauges := []ResourceGauge{NewResourceGauge("route_claims", 1_000, 184)}
	usage := NewSubsystemUsage(ResourceSubsystemRoutePlane, gauges...)
	gauges[0] = NewResourceGauge("rewritten", 1, 1)
	if got := usage.Gauges()[0].Name(); got != "route_claims" {
		t.Fatalf("the subsystem aliased its caller's slice: gauge is now %q", got)
	}

	subsystems := []SubsystemUsage{usage}
	breakdown := NewResourceBreakdown(time.Now().UTC(), subsystems...)
	subsystems[0] = NewSubsystemUsage(ResourceSubsystemBans)
	if got := breakdown.Subsystems()[0].Subsystem(); got != ResourceSubsystemRoutePlane {
		t.Fatalf("the breakdown aliased its caller's slice: subsystem is now %v", got)
	}
}

// TestSubsystemNamesAreStable pins the metric labels: they are what dashboards
// and the step's own tables key on, so renaming one silently would break every
// comparison against a number recorded before the rename.
func TestSubsystemNamesAreStable(t *testing.T) {
	t.Parallel()

	want := map[ResourceSubsystem]string{
		ResourceSubsystemRoutePlane: "route_plane",
		ResourceSubsystemAnnounce:   "announce",
		ResourceSubsystemDatagram:   "datagram",
		ResourceSubsystemDelivery:   "delivery",
		ResourceSubsystemSessions:   "sessions",
		ResourceSubsystemKnowledge:  "knowledge",
		ResourceSubsystemBans:       "bans",
		ResourceSubsystemOverlay:    "overlay",
	}
	for subsystem, name := range want {
		if got := subsystem.String(); got != name {
			t.Fatalf("ResourceSubsystem(%d).String() = %q, want %q", subsystem, got, name)
		}
		if !subsystem.Valid() {
			t.Fatalf("%s must be a valid subsystem", name)
		}
	}
	if ResourceSubsystemUnset.Valid() {
		t.Fatal("the zero subsystem must not be valid: an unassigned gauge belongs to nobody")
	}
}

// TestResourceSubsystemBoundEnclosesEveryNamedValue pins the enumeration bound
// the coverage guard in the node walks.
//
// The node test can only be as complete as the range it iterates, so the range
// itself is asserted here: every value below the bound has a name and is
// valid, the bound and the zero value are not, and no named value lives past
// the bound. The last clause is what catches a constant appended AFTER the
// bound — it would carry a name, fail Valid, and drop out of every walk
// without a single other test noticing.
func TestResourceSubsystemBoundEnclosesEveryNamedValue(t *testing.T) {
	t.Parallel()

	for subsystem := ResourceSubsystemUnset + 1; subsystem < resourceSubsystemBound; subsystem++ {
		if _, named := resourceSubsystemNames[subsystem]; !named {
			t.Fatalf("ResourceSubsystem(%d) lies below the bound but has no name", subsystem)
		}
		if !subsystem.Valid() {
			t.Fatalf("%s lies below the bound but is not valid", subsystem)
		}
	}
	if resourceSubsystemBound.Valid() {
		t.Fatal("the bound is a marker, not a subsystem")
	}
	if ResourceSubsystemUnset.Valid() {
		t.Fatal("the zero subsystem must not be valid")
	}
	for subsystem, name := range resourceSubsystemNames {
		if subsystem == ResourceSubsystemUnset {
			continue
		}
		if !subsystem.Valid() {
			t.Fatalf("%q is named but not valid: a constant declared past the bound is invisible to every walk over the enum",
				name)
		}
	}
}

// TestResourceSubsystemsListsTheWholeRange keeps the exported walk and the
// bound in step: the node's coverage guard iterates ResourceSubsystems, so a
// list that stopped short would turn the guard into a check of a subset.
func TestResourceSubsystemsListsTheWholeRange(t *testing.T) {
	t.Parallel()

	listed := ResourceSubsystems()
	if len(listed) != int(resourceSubsystemBound)-1 {
		t.Fatalf("ResourceSubsystems() has %d entries, the bound encloses %d", len(listed), int(resourceSubsystemBound)-1)
	}
	for i, subsystem := range listed {
		if want := ResourceSubsystem(i + 1); subsystem != want {
			t.Fatalf("ResourceSubsystems()[%d] = %s, want %s", i, subsystem, want)
		}
	}
}
