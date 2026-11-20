# Disposable observer diagnostic: FullSend family

This is a complement to the evidence committed in `f258093a44aa15dd7ce79f6e6a13df9c0aae363c`, [observer-first-failure](../d1-observer-first-failure-2026-11-20/README.md). The unchanged observer posts, three coherent namespace fixture posts and original dependency lineage remain there.

`prepare_evidence.py` reads fixed retained origins only. ROOT assembles the bundle; it does not build or run applications. RAW contains every command stdout/stderr/process record from the coherent four-ELF build, source preparation, two helper builds, initrd assembly and runtime binding, plus the complete native run and all three stopped-only reviews/ACK. SOURCE contains exact new controllers, deltas, input inventories, source manifests and bindings. Application binaries, initrds, caches and private key/profile files are excluded.

The complete `native/console.raw` preserves the encoded capsule of **1351 files**: 1348 Go sources, two manifests and one failed record. `CAPSULE-MEMBERS.json` indexes every expanded member; the generator checks all Go payload hashes against the compiled source manifest. The two manifests and failed record are also retained directly in RAW. Expanded Go source bytes are not duplicated outside that complete transport.

Attempt `c01f0e036e3ea01e020dee5d88dd8564` failed the original Go test after **30.63 s**, with stage **`observer_full_send`**, transport code **72**. The failed record has no body. The specific sender site or compound predicate is **UNKNOWN**. Code72 does not prove that this is the cause of either previous 30 s or 480 s failure.

Setup advanced beyond the preceding initial multicast guard. This observation does not establish general environment readiness. Test exit1 and exporter exit0 were normal/reaped; native worker exit0 and all four lifecycle events were retained. Three independent reviews authorized only the exact owned stopped-resource cleanup. Physical XPC state remains **UNKNOWN**. The changed private Core is diagnostic instrumentation: no baseline, bandwidth, 512 MiB or whole-D1 acceptance is claimed.

A separate focused unit run passed **four outer tests and 12 child cases** with one command and 17,207 RAW bytes. The unchanged 2105 Go/module records and one virtual test post are retained, together with the relevant five-source proof. It exercises the original sender/queue with controlled window crossing and changed same-connection projection inputs. This demonstrates possible refusal mechanisms; it does not identify the actual c01 condition.

Archives use regular USTAR members, mode0644, UID/GID/mtime0 and no PAX. RAW/SOURCE member inventories and SHA256SUMS support complete byte readback. The generator has fixed file/member/payload limits and includes its own source in the top-level checksum manifest.
