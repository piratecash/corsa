package node

import (
	"os"
	"strconv"
	"strings"
)

// cgroupUnlimitedThreshold is the floor above which a cgroup v1 memory
// limit is treated as "no limit". cgroup v1 encodes unlimited as a very
// large page-counter sentinel (typically 0x7FFFFFFFFFFFF000 ≈ 9.2e18);
// any limit at or above 1<<62 is, for practical purposes, unbounded.
const cgroupUnlimitedThreshold uint64 = 1 << 62

// readCgroupMemory returns the memory limit and current usage in bytes
// read from the ROOT of the mounted cgroup hierarchy — the fixed paths
// /sys/fs/cgroup/memory.{max,current} (v2) or
// /sys/fs/cgroup/memory/memory.{limit_in_bytes,usage_in_bytes} (v1). It
// deliberately does NOT resolve the process's own cgroup via
// /proc/self/cgroup.
//
// This is a container-oriented best-effort, NOT a guaranteed
// "this process's cgroup" reading:
//   - Inside a container with a private cgroup namespace (Docker/k8s
//     default) the mount root IS the container's cgroup, so these are
//     the container's memory and the limit the OOM killer enforces —
//     exactly the intended figure.
//   - On a bare host, a systemd service, or a non-private cgroup
//     namespace, the mount root is a broad/root cgroup, so the figures
//     describe that wider scope, not the corsa process specifically.
//
// We do not resolve+reject the root path because inside a private
// namespace the container legitimately appears AT the root ("/"), so a
// root-rejecting heuristic would break the common container case while
// only marginally helping the uncommon bare-host one. The figures are
// reported as-is and documented (domain.ResourceUsage) as "current
// cgroup usage", meaningful primarily for container/service cgroups.
//
// limit returns 0 when the process is not under a cgroup memory
// controller, the files are unreadable, or the limit is "max" / an
// unlimited sentinel. usage returns 0 only when it is genuinely
// unreadable — an unlimited LIMIT does not suppress the usage read.
//
// Best-effort and allocation-light: a couple of small os.ReadFile calls.
func readCgroupMemory() (limitBytes, usageBytes uint64) {
	// cgroup v2 (unified hierarchy).
	if limit, ok := readCgroupV2Max("/sys/fs/cgroup/memory.max"); ok {
		usage, _ := readUintFile("/sys/fs/cgroup/memory.current")
		return limit, usage
	}
	// cgroup v1.
	if limit, ok := readUintFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); ok {
		if limit >= cgroupUnlimitedThreshold {
			limit = 0 // unlimited sentinel
		}
		usage, _ := readUintFile("/sys/fs/cgroup/memory/memory.usage_in_bytes")
		return limit, usage
	}
	return 0, 0
}

// readCgroupV2Max reads a cgroup v2 limit file whose value is either a
// byte count or the literal "max" (unlimited). Returns (0, true) for
// "max" so the caller can still read usage; (0, false) when the file is
// absent/unreadable (signal to fall through to v1).
func readCgroupV2Max(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, true // present, but unlimited
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// readUintFile parses a single unsigned integer from a sysfs/cgroup
// file. Returns ok=false on any read or parse error.
func readUintFile(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
