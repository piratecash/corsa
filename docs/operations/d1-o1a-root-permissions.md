# O1a guest-root permissions

The private O1a test guest inherited a root directory owned by UID0 with mode01777. For a nonroot helper, the unchanged secretfile rule rejects root-owned `/profiles` beneath that shared sticky ancestor. A C diagnostic reproduced this environment incompatibility; the original Go first failing predicate remains UNKNOWN.

The countercheck changed only the fresh guest root to0755 before mounts and children. Actual before/after metadata retained UID/GID0 and dev/inode; the same probe then passed all180 FS observations and four location audits under UID/GID65532, zero effective capabilities, NNP1 and the original seccomp filter. Both diagnostic VMs stopped normally and all children were reaped. Permission/ACL predicates, caps, kernel, product binaries and workload criteria were preserved.

The corresponding private Go PID1 fix passed10 host unit functions and Linuxarm64/CGO0 compile/metadata checks. The build-time compile-only scope is preserved; the later execution is described below. This closes the demonstrated environment bug and C countercheck; **main original Go/O1a remains NOT_PASS_YET**. It grants no D1/G2/public activation or release readiness.

[Portable evidence and scope](evidence/d1-o1a-root-permissions-2026-11-20/README.md) · [Result](evidence/d1-o1a-root-permissions-2026-11-20/RESULT.json) · [Hashes](evidence/d1-o1a-root-permissions-2026-11-20/SHA256SUMS).

A later original periodic attempt `651767f4` ran the new Go PID1 and failed before target creation on the same kernel: setup-network guard4 rejected `ff00::/8` with `configure=false`. The Go test ended normally with exit1, no journal/export or measurement; native stop/reap and three-actor cleanup ACK completed. This is a separate network-qualification block. The successful C root-permission countercheck stands; the original first private-profile predicate remains UNKNOWN and O1a is NOT_PASS.
