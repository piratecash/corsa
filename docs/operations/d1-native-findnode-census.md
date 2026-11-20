# Closed native FIND_NODE candidate with census

The `d1_o1a_native_responder` tag selects a separate FullResponder entry for
the native fixture. It composes the original RuntimeDomain/Ready and census
in the same first Service before constructor callbacks. Request scalars are
copied; source/identity/lifetime replacement, recapture and overwritten refusal
cannot finalize Ready. Ordinary constructors remain Off; the existing
loopback/small2 diagnostic is unchanged.

The profile reuses the reviewed SG512 allocation: active512, members512,
registry visits1024 and the same timing/signing. The native fixture retains
two Full nodes, 10,000 routes and original dial slots1/0. These are constructor
parameters, not a successful 512-session load result.

The existing live census callback verifies genuine published F1 for that same
Ready Service through original token/lifetime/ConnID/peer/direction/exporter
bindings. Retired historical rows keep their original V2-only meaning. Generic
post-stop census, report schema, RPC commands and route-wire traffic remain
unchanged. No FIND_NODE requests are injected into the quiet scenario.

Persistent ownIntro hash/flags were deliberately omitted as unnecessary for
this experiment; `find_node_connection.go` remains byte-identical. A genuine TLS
unit checks peer-observed F1/R. Actual R on every future runtime session is
**UNKNOWN**, and forwarding is **NOT_EXERCISED**. The scope is
**ENABLED_IDLE_CONSTRUCTION_COST**, not proof of every wire path.

The isolated `2188dfb8fcb74174fa4d1df2084784bd07d436bf` plus exact26-post snapshot
passed **164 named executions and353 child cases**, without FAIL/SKIP.
All20 commands exited0, all2642 source files remained unchanged, own processes
were absent and the lock was released. Node/App test ELF and FullNode main were
compiled Linux arm64/CGO0 with Go1.27.4, verified by SHA/static/BuildInfo, and not
executed. Two earlier finite-inventory failures are preserved. Corrections only
updated an exact raw-body pin, an exact tagged importer case and the driver's
existing Service pin; scanners and negative controls remain enabled.

[Portable evidence](evidence/d1-native-responder-2026-11-20/README.md).
Current-candidate runtime measurement remains pending. The [baseline census
failure](d1-o1a-baseline-census.md) remains separate. This QA closes neither
O1a, standalone512MB, remaining13b-D1, G2, public activation nor release.
