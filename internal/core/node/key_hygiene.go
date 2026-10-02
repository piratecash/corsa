package node

import "github.com/piratecash/corsa/internal/core/protocol"

// keyHygieneStats gathers the counters of key material this node refused to
// use. Every one of those refusals is silent on the wire, so these numbers
// are the operator's only view of them; they ride fetch_network_stats.
// Lock-free: immutable-after-New fields and atomics only, so the hot-read
// contract of that RPC holds.
func (s *Service) keyHygieneStats() protocol.KeyHygieneFrame {
	return protocol.KeyHygieneFrame{
		RefusedTrustedContacts: s.trustedKeysAtLoad.refused,
		DroppedTrustedBoxPairs: len(s.trustedKeysAtLoad.droppedBoxPairs),
		UnattributedNonDMDrops: s.unattributedNonDMDrops.Load(),
		NonDMKeySyncPasses:     s.nonDMKeySyncPasses.Load(),
		NonDMKeySyncSkipped:    s.nonDMKeySyncSkipped.Load(),
	}
}
