package domain

import "time"

// NodeStatus is an immutable point-in-time snapshot of the local Corsa
// node's identity and liveness, returned by the getNodeStatus RPC
// endpoint.
//
// It is the integration surface defined by PIP-0001 ("Masternode
// Messenger Service Integration"): PirateCash Core uses this snapshot
// at masternode startup (Stage 1), as a liveness/version probe (Stage
// 2), and as the source of the public key the v21 proof-of-service
// will be tied to (Stage 3).
//
// Wire contract:
//
//   - Field names use explicit snake_case JSON tags. The struct is part
//     of an external integration; renaming fields silently would break
//     PirateCash Core checks. Add new fields, never repurpose names.
//   - All key material is PUBLIC. The struct intentionally does NOT
//     carry private keys, seeds, RPC credentials, or any other secret —
//     leaking those would defeat the integration's threat model. Any
//     future field MUST honor the same invariant.
//   - Stages 1 and 2 do not require a signature on the response.
//     Stage 3 (v21 PoSe) will introduce a separately-fetched signed
//     proof-of-service object; this snapshot stays signature-free so
//     it remains cheap to call from health-check loops.
type NodeStatus struct {
	// Identity is the node identity fingerprint (sha256 over the
	// ed25519 public key, hex-encoded). Stable for the lifetime of the
	// keypair and used as the routing identifier across the messenger
	// network. This is the PIP-0001-facing field name PirateCash Core
	// should consume.
	Identity string `json:"identity"`

	// Address is the Corsa-native name for the same identity
	// fingerprint. Kept alongside Identity because existing Corsa APIs
	// and docs use "address" for the local routing identifier.
	Address string `json:"address"`

	// PublicKey is the base64-encoded ed25519 public key that backs
	// Address. The v21 proof-of-service will sign challenges with the
	// matching private key; PirateCash Core verifies signatures against
	// the value reported here.
	PublicKey string `json:"public_key"`

	// BoxPublicKey is the base64-encoded NaCl curve25519 public key used
	// for end-to-end DM encryption. Returned so PirateCash Core can pin
	// the full identity material in a single call without a follow-up
	// fetch_contacts round-trip.
	BoxPublicKey string `json:"box_public_key"`

	// ProtocolVersion is the wire protocol version this node currently
	// speaks (config.ProtocolVersion). Stage 2 uses this to refuse
	// masternodes that fall behind the network.
	ProtocolVersion int `json:"protocol_version"`

	// MinimumProtocolVersion is the lowest peer protocol version this
	// node will accept on the wire (config.MinimumProtocolVersion).
	// Useful for PirateCash Core to detect impending hard cutovers.
	MinimumProtocolVersion int `json:"minimum_protocol_version"`

	// ClientVersion is the Corsa node implementation version
	// (Service.ClientVersion()). Free-form for informational logging on
	// the PirateCash side.
	ClientVersion string `json:"client_version"`

	// ClientBuild is the integer build identifier (config.ClientVersionBuild)
	// monotonically bumped per release. Easier to compare than
	// ClientVersion when policy gates have to fire on a specific build.
	ClientBuild int `json:"client_build"`

	// ConnectedPeers is the number of **distinct peer identities** the
	// node currently has at least one live connection with, counting
	// both outbound and inbound sessions. Multiple sockets from the
	// same peer identity collapse to one — the field measures relay
	// reach, not socket count. Stage 2 may use this as a service-health
	// signal: a masternode reporting zero usable peers is unlikely to
	// be relaying messenger traffic.
	//
	// For per-connection detail (separate inbound/outbound rows,
	// remote addresses, slot lifecycle), use the getActiveConnections
	// command instead.
	ConnectedPeers int `json:"connected_peers"`

	// StartedAt is the wall-clock time the Service was constructed
	// (RFC3339Nano, UTC). Combined with CurrentTime it provides
	// uptime without requiring the caller to trust a single clock.
	StartedAt time.Time `json:"started_at"`

	// UptimeSeconds is the number of whole seconds elapsed since
	// StartedAt. Convenience field: equivalent to
	// CurrentTime.Sub(StartedAt) but avoids re-implementing the
	// arithmetic in every PirateCash health probe.
	UptimeSeconds int64 `json:"uptime_seconds"`

	// CurrentTime is the node's wall-clock time at the moment the
	// snapshot was generated (RFC3339Nano, UTC). PirateCash Core uses
	// this to detect drift between its own clock and the masternode's.
	CurrentTime time.Time `json:"current_time"`
}

// ResourceUsage is a point-in-time snapshot of the process's memory
// footprint (runtime.MemStats: sys, heap alloc/inuse/idle/released, GC
// metadata), the cgroup memory limit + usage, the live connection
// count, and uptime — returned by the getResourceUsage RPC command. The
// desktop console's Info tab renders the headline memory/uptime subset;
// the rest is for dashboards and leak/OOM diagnostics. Both machine-
// readable (raw integers) and human-readable (pre-formatted strings)
// representations are included so dashboards consume the numbers while
// operators reading the JSON or the UI get sensible units.
//
// Memory is sourced from runtime.MemStats (the Go standard-library
// mechanism). MemSysBytes is the total obtained from the OS — the
// closest runtime-visible proxy for process footprint / RSS;
// MemHeapAllocBytes is the live (in-use) heap, the figure that grows
// under a leak.
type ResourceUsage struct {
	// MemSysBytes is runtime.MemStats.Sys — total bytes of memory
	// obtained from the OS. Best single proxy for process footprint.
	MemSysBytes uint64 `json:"mem_sys_bytes"`
	// MemSysHuman is MemSysBytes formatted with a B/KB/MB/GB/TB unit.
	MemSysHuman string `json:"mem_sys_human"`

	// MemHeapAllocBytes is runtime.MemStats.HeapAlloc — bytes of
	// live (reachable + not-yet-collected) heap objects. The figure
	// that climbs steadily under a memory leak.
	MemHeapAllocBytes uint64 `json:"mem_heap_alloc_bytes"`
	// MemHeapAllocHuman is MemHeapAllocBytes formatted with a unit.
	MemHeapAllocHuman string `json:"mem_heap_alloc_human"`

	// HeapInuseBytes is runtime.MemStats.HeapInuse — bytes in in-use
	// heap spans (>= HeapAlloc; includes free slots within spans that
	// still hold live objects). Gap vs HeapAlloc indicates fragmentation.
	HeapInuseBytes uint64 `json:"heap_inuse_bytes"`
	HeapInuseHuman string `json:"heap_inuse_human"`

	// HeapIdleBytes is runtime.MemStats.HeapIdle — bytes in idle (unused)
	// heap spans, available for reuse or eventual release to the OS. Large
	// idle after a memory spike means the runtime is holding reclaimed
	// memory it hasn't returned yet (HeapReleased is the released subset).
	HeapIdleBytes uint64 `json:"heap_idle_bytes"`
	HeapIdleHuman string `json:"heap_idle_human"`

	// HeapReleasedBytes is runtime.MemStats.HeapReleased — bytes of idle
	// heap returned to the OS. Rising HeapReleased after a spike is the
	// runtime giving memory back; the difference HeapIdle-HeapReleased is
	// reclaimed-but-still-held.
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	HeapReleasedHuman string `json:"heap_released_human"`

	// GCSysBytes is runtime.MemStats.GCSys — bytes of memory used by the
	// garbage collector's own metadata. Grows with heap size and churn.
	GCSysBytes uint64 `json:"gc_sys_bytes"`
	GCSysHuman string `json:"gc_sys_human"`

	// CgroupMemLimitBytes / CgroupMemUsageBytes are the memory limit and
	// current usage read from the ROOT of the mounted cgroup hierarchy —
	// the fixed paths /sys/fs/cgroup/memory.{max,current} (v2) or
	// /sys/fs/cgroup/memory/memory.{limit_in_bytes,usage_in_bytes} (v1).
	// The process's own cgroup is NOT resolved via /proc/self/cgroup.
	//
	// Scope caveat — this is container-oriented best-effort, NOT a
	// guaranteed per-process reading:
	//   - Inside a container with a private cgroup namespace (Docker/k8s
	//     default) the mount root IS the container's cgroup, so the
	//     figures are the container's memory and the limit the OOM killer
	//     enforces — exactly the intended use.
	//   - On a bare host, a systemd service, or a non-private cgroup
	//     namespace, the mount root is a broad / machine-root cgroup, so
	//     the figures describe that wider scope, NOT this process or
	//     service. Do not read them as the corsa process's memory there.
	// Accurate for Docker/k8s private cgroup namespaces; broad/root scope
	// elsewhere.
	//
	// CgroupMemLimitBytes is 0 (CgroupMemLimitHuman "unlimited") when no
	// cgroup memory controller is mounted at the root, the files are
	// unreadable, or the limit is "max"/unbounded. An unlimited limit
	// zeroes only the LIMIT — CgroupMemUsageBytes is still read and
	// reported. CgroupMemUsageBytes is 0 only when usage is genuinely
	// unreadable / no controller mounted.
	CgroupMemLimitBytes uint64 `json:"cgroup_mem_limit_bytes"`
	CgroupMemLimitHuman string `json:"cgroup_mem_limit_human"`
	CgroupMemUsageBytes uint64 `json:"cgroup_mem_usage_bytes"`
	CgroupMemUsageHuman string `json:"cgroup_mem_usage_human"`

	// ConnectionCount is the number of live peer connections (inbound +
	// outbound) the node currently holds — the same liveness set as
	// getActiveConnections. A footprint that grows in lock-step with this
	// is working set, not a leak.
	ConnectionCount int `json:"connection_count"`

	// ShadowDivergenceTotal is the Phase 3 deploy-1 cumulative count of
	// announce delta-cursor shadow mismatches — delta entries the change
	// journal failed to cover. A canary that holds this at 0 over sustained
	// churn proves the journal complete before the cursor model is made
	// authoritative (deploy-2); a non-zero, growing value flags an unwired
	// mutation site. Temporary observability — removed with the shadow stage.
	ShadowDivergenceTotal uint64 `json:"shadow_divergence_total"`

	// UptimeSeconds is whole seconds since process start.
	UptimeSeconds int64 `json:"uptime_seconds"`
	// UptimeHuman is the uptime rendered in the largest of three
	// tiers — seconds (< 1 h), hours (< 1 day), or days.
	UptimeHuman string `json:"uptime_human"`

	// SampledAt is the wall-clock instant the sample was taken
	// (RFC3339Nano, UTC).
	SampledAt time.Time `json:"sampled_at"`
}
