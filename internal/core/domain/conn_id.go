package domain

// ConnID is the typed identifier for an active peer connection. It is
// stable for the lifetime of a single socket and carries no semantic
// beyond identity: two ConnID values being equal means "the same live
// connection", nothing more. The primary connection registry is keyed
// by ConnID; every call site that previously carried the identifier as
// a raw uint64 uses this type instead so mismatches between session,
// registry and transport surface become compile-time errors rather
// than silent lookups against the wrong map.
type ConnID uint64
