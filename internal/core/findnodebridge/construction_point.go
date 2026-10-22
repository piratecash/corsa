package findnodebridge

// MatchesPoint compares immutable original clock provenance, including during
// Constructing. It grants neither a CaptureStamp nor current R/T authority.
// Only an actual original Observer clock can produce a valid matching point.
func (c Consumer) MatchesPoint(at OwnedInstant) bool {
	b := c.consumerBinding
	return b.valid(consumerRole) && at.valid() && b.owner == at.instantDomain && b.kind == at.instantKind
}
