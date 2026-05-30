package routing

// anti_poison_test.go intentionally carries no test functions.
//
// The original Phase 4 13.6 design proposed an admission gate that
// rejected incoming RouteSourceAnnouncement claims when an existing
// RouteSourceDirect / RouteSourceHopAck claim for the same
// (Identity, Uplink) slot was "fresh" (within DefaultTTL/2 of now).
// Implementation revealed the rule conflicted with the documented
// reconfirmation contracts:
//
//   - weaker_source_refreshes_ttl_marks_dirty
//     (table_dirty_test.go) — a HopAck claim re-confirmed by a
//     subsequent forced-full-sync Announcement at the same SeqNo
//     and Hops must refresh TTL (RouteUnchanged + dirty),
//     otherwise hop_ack-promoted routes age out by TTL despite
//     being actively re-announced.
//   - TestUpdateRoute_HopAckPromotionPreservesWireLineage
//     (table_test.go) — the forced-full-sync re-announce after a
//     hop_ack promotion is a legitimate refresh; rejecting it
//     breaks the "promotion sticks" semantic.
//
// Root-cause analysis. The phase doc's anti-poisoning bullet was
// drafted before the Phase 1 per-(Identity, Uplink) storage
// refactor. The poisoning vector it targeted — a third-party
// peer's Announcement overriding our direct / hop_ack-confirmed
// claim — is structurally impossible after Phase 1: third-party
// claims land in a SEPARATE bucket slot (different uplink), and
// the local Direct / HopAck claim stays untouched in its own slot.
// The same-uplink case my draft rule actually fired on is the
// legitimate same-upstream re-announce / TTL refresh path, not a
// poisoning attack. See the Phase 4 doc entry for §3.6 for the
// final finding.
//
// If a future threat model surfaces a poisoning vector that the
// per-uplink storage does NOT cover (e.g. authenticated peer
// impersonation combined with a stale-key attack), that work lands
// as a fresh PR with a sharply-scoped admission gate and
// regression tests for the legitimate refresh paths the original
// rule broke.
