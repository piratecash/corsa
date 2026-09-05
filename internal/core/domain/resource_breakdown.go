package domain

import (
	"fmt"
	"reflect"
	"time"
)

// resource_breakdown.go answers the question ResourceUsage cannot.
//
// ResourceUsage reports runtime.MemStats — the whole process. That is enough
// to SEE growth and never enough to say WHOSE it is, and "whose" is the only
// form of the answer a resource budget can be built on: a release that costs
// 40 MB is acceptable or not depending on which subsystem spends it and
// whether that subsystem's growth is bounded.
//
// The shape here is deliberately counts-first. Every gauge reports an EXACT
// cardinality — a container's len, read where the owner already holds its
// lock — plus the size of one entry, and never a walk over the entries
// themselves: an accounting pass that scanned the routing table under its
// mutex would change the thing it measures (13-measurements.md §4).
//
// The byte figure that follows from those two numbers is therefore a FLOOR and
// is named one. It counts the key and the value a container stores and nothing
// else: not Go's per-bucket map overhead, and not the bytes a stored value
// merely POINTS AT — a signature, an opaque Extra blob, a nested slice. Real
// consumption is higher, by a factor that differs per container. This is the
// honest trade: a floor computed from exact counts beats an estimate computed
// from a walk nobody can afford to run.
//
// Rendering — human-readable units, JSON field names — is deliberately absent.
// It belongs to the node's diagnostic surface, the same way the datagram route
// plan is rendered where it is served rather than where it is produced.
//
// Reference: docs/refactoring/dht/13-measurements.md §2, §4, §5.

// ResourceSubsystem names one owner of long-lived state.
//
// The list is the answer to "who could be holding it", so it is short and
// every member is a thing an operator can act on: a subsystem nobody can
// bound, disable or resize does not belong here, and two subsystems that are
// always resized together belong on one line.
type ResourceSubsystem uint8

const (
	// ResourceSubsystemUnset is the zero value and never a valid answer.
	ResourceSubsystemUnset ResourceSubsystem = iota
	// ResourceSubsystemRoutePlane is the distance-vector routing table:
	// claims, per-identity buckets, health and the SeqNo bookkeeping that
	// grows as identities × peers.
	ResourceSubsystemRoutePlane
	// ResourceSubsystemAnnounce is what the announce loop keeps PER PEER —
	// above all the last snapshot sent to each of them, whose contents are
	// proportional to the table.
	ResourceSubsystemAnnounce
	// ResourceSubsystemDatagram is the datagram plane: class queues, the
	// anti-replay cache, reverse-state records, per-neighbour budgets.
	ResourceSubsystemDatagram
	// ResourceSubsystemDelivery is message delivery: pending and outbound
	// queues, retry bookkeeping, receipts, the transit backlog.
	ResourceSubsystemDelivery
	// ResourceSubsystemSessions is what a live connection costs: sockets,
	// their fixed buffers, and the per-peer records keyed by address.
	ResourceSubsystemSessions
	// ResourceSubsystemKnowledge is the cache of known identities and their
	// key material.
	ResourceSubsystemKnowledge
	// ResourceSubsystemBans is the IP-level ban and observation state. It is
	// its own line because it has already been the answer once: a leak
	// traced to ban maps that were only cleaned lazily.
	ResourceSubsystemBans
)

var resourceSubsystemNames = map[ResourceSubsystem]string{
	ResourceSubsystemUnset:      "unset",
	ResourceSubsystemRoutePlane: "route_plane",
	ResourceSubsystemAnnounce:   "announce",
	ResourceSubsystemDatagram:   "datagram",
	ResourceSubsystemDelivery:   "delivery",
	ResourceSubsystemSessions:   "sessions",
	ResourceSubsystemKnowledge:  "knowledge",
	ResourceSubsystemBans:       "bans",
}

// String returns the stable metric label of the subsystem.
func (s ResourceSubsystem) String() string {
	if name, ok := resourceSubsystemNames[s]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", s)
}

// Valid reports whether the value names a real subsystem.
func (s ResourceSubsystem) Valid() bool {
	_, ok := resourceSubsystemNames[s]
	return ok && s != ResourceSubsystemUnset
}

// SizeOfAll sums the in-memory footprint of the given zero values — the
// per-entry cost of a container that stores them.
//
// It is called once, at package initialisation of whoever declares a gauge,
// and never on a read path. Callers pass the key and the value a map stores,
// or the element a slice stores; for a map whose value is a POINTER they pass
// the pointee, because the pointee is where the bytes are and the eight bytes
// of the pointer itself are noise at the scale this measures.
//
// reflect rather than unsafe.Sizeof on purpose: the number is identical, and
// unsafe is kept in this tree for the platform files that genuinely need it,
// where its presence in an import list still means something.
func SizeOfAll(values ...any) uint64 {
	var total uint64
	for _, value := range values {
		total += uint64(reflect.TypeOf(value).Size())
	}
	return total
}

// ResourceGaugeKind separates the two questions a gauge can answer, because
// summing them is a real arithmetic error and not a stylistic one.
//
// A MEMORY gauge counts entries nobody else counts, so its bytes may be added
// to a total. A SATURATION gauge counts how full a quota is — and the entries
// it counts are, by construction, a SUBSET of entries some memory gauge has
// already counted. Adding it would report the same records twice and break the
// one promise this whole surface makes: that the figure is a floor.
type ResourceGaugeKind uint8

const (
	// ResourceGaugeMemory counts entries whose bytes belong in the total.
	ResourceGaugeMemory ResourceGaugeKind = iota
	// ResourceGaugeSaturation counts occupancy of a quota. It contributes NO
	// bytes: the records behind it are already counted elsewhere, and the
	// number is here to be read against a limit rather than added to a sum.
	ResourceGaugeSaturation
)

// String returns the metric label of the kind.
func (k ResourceGaugeKind) String() string {
	if k == ResourceGaugeSaturation {
		return "saturation"
	}
	return "memory"
}

// ResourceGauge is ONE container's live cardinality and what one of its
// entries costs.
//
// The fields are unexported and read through accessors so a consumer cannot
// rewrite a count after the fact and make a diagnostic disagree with the
// container it describes.
type ResourceGauge struct {
	name       string
	count      uint64
	entryBytes uint64
	kind       ResourceGaugeKind
}

// NewResourceGauge records one container whose bytes belong in the total.
// count is what its len answered; entryBytes is what SizeOfAll said one entry
// costs.
//
// A negative count becomes zero rather than an enormous unsigned number: len
// cannot return one, but a caller computing a difference can, and a diagnostic
// that reported four exabytes of route claims would be read as a leak.
func NewResourceGauge(name string, count int, entryBytes uint64) ResourceGauge {
	if count < 0 {
		count = 0
	}
	return ResourceGauge{name: name, count: uint64(count), entryBytes: entryBytes}
}

// NewSaturationGauge records how full a quota is, contributing no bytes.
//
// It exists because the alternative — publishing an occupancy as an ordinary
// gauge — double-counted records the containing table had already reported,
// and did it invisibly: both numbers were individually correct and their sum
// was not.
func NewSaturationGauge(name string, count int) ResourceGauge {
	if count < 0 {
		count = 0
	}
	return ResourceGauge{name: name, count: uint64(count), kind: ResourceGaugeSaturation}
}

// Kind reports whether this gauge's bytes count towards a total.
func (g ResourceGauge) Kind() ResourceGaugeKind { return g.kind }

// Name is the stable metric label of the container.
func (g ResourceGauge) Name() string { return g.name }

// Count is the exact number of entries the container held when sampled.
func (g ResourceGauge) Count() uint64 { return g.count }

// EntryBytes is what one entry costs — key plus value, excluding what they
// point at.
func (g ResourceGauge) EntryBytes() uint64 { return g.entryBytes }

// FloorBytes is count × entry, and the name says what it is worth: a lower
// bound, never a measurement. See the file comment for what it leaves out.
//
// A saturation gauge answers zero: the records it counts are a subset of ones
// already counted by the container they live in, and charging for them twice
// would make a "floor" larger than the truth it claims to sit under.
func (g ResourceGauge) FloorBytes() uint64 {
	if g.kind == ResourceGaugeSaturation {
		return 0
	}
	return g.count * g.entryBytes
}

// SubsystemUsage is everything one subsystem holds.
type SubsystemUsage struct {
	gauges    []ResourceGauge
	subsystem ResourceSubsystem
}

// NewSubsystemUsage records one subsystem's gauges. The slice is copied: the
// caller assembles it from live containers and must not be able to keep
// writing into a value a diagnostic is already rendering.
func NewSubsystemUsage(subsystem ResourceSubsystem, gauges ...ResourceGauge) SubsystemUsage {
	return SubsystemUsage{
		subsystem: subsystem,
		gauges:    append([]ResourceGauge(nil), gauges...),
	}
}

// Subsystem names the owner.
func (u SubsystemUsage) Subsystem() ResourceSubsystem { return u.subsystem }

// Gauges returns the containers in the order the owner listed them — most
// significant first, so a reader who stops at the first line has read the one
// that matters. The slice is a copy.
func (u SubsystemUsage) Gauges() []ResourceGauge {
	return append([]ResourceGauge(nil), u.gauges...)
}

// FloorBytes sums the subsystem's gauges.
func (u SubsystemUsage) FloorBytes() uint64 {
	var total uint64
	for _, gauge := range u.gauges {
		total += gauge.FloorBytes()
	}
	return total
}

// ResourceBreakdown is the whole picture at one instant.
type ResourceBreakdown struct {
	sampledAt  time.Time
	subsystems []SubsystemUsage
}

// NewResourceBreakdown assembles the sample.
func NewResourceBreakdown(sampledAt time.Time, subsystems ...SubsystemUsage) ResourceBreakdown {
	return ResourceBreakdown{
		sampledAt:  sampledAt,
		subsystems: append([]SubsystemUsage(nil), subsystems...),
	}
}

// SampledAt is when the counts were read. The subsystems are NOT sampled under
// one lock — each is read where its owner already holds its own — so this is
// the instant the pass began and the picture is consistent only to within it.
// That is the right trade for a diagnostic: a global lock across every
// subsystem to make one number tidy would stall the node it is measuring.
func (b ResourceBreakdown) SampledAt() time.Time { return b.sampledAt }

// Subsystems returns the sample, one entry per owner. The slice is a copy.
func (b ResourceBreakdown) Subsystems() []SubsystemUsage {
	return append([]SubsystemUsage(nil), b.subsystems...)
}

// FloorBytes sums every subsystem.
func (b ResourceBreakdown) FloorBytes() uint64 {
	var total uint64
	for _, usage := range b.subsystems {
		total += usage.FloorBytes()
	}
	return total
}

// Dominant returns the subsystem holding the most, which is the question the
// whole type exists to answer: a budget is set against the dominating
// consumer, and "named with a number" is what separates a measurement from a
// hypothesis.
//
// The bool is false for an empty breakdown and for one in which everything is
// zero — a node that has just started holds nothing, and naming an arbitrary
// subsystem as its dominant consumer would be an answer with no content. Ties
// are broken by the assembly order, which is stable.
func (b ResourceBreakdown) Dominant() (SubsystemUsage, bool) {
	var best SubsystemUsage
	var bestBytes uint64
	for _, usage := range b.subsystems {
		if bytes := usage.FloorBytes(); bytes > bestBytes {
			best, bestBytes = usage, bytes
		}
	}
	if bestBytes == 0 {
		return SubsystemUsage{}, false
	}
	return best, true
}
