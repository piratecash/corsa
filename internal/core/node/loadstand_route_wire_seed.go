package node

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

const loadstandRouteWireSeedPrefix = "corsa-o1a-seed!!"

func loadstandRouteWireSeedIdentity(index uint32) domain.PeerIdentity {
	var id domain.PeerIdentity
	copy(id[:16], loadstandRouteWireSeedPrefix)
	binary.BigEndian.PutUint32(id[16:], index)
	return id
}

func loadstandRouteWireSeedDigest(count int) string {
	digest := sha256.New()
	for i := 1; i <= count; i++ {
		id := loadstandRouteWireSeedIdentity(uint32(i))
		_, _ = digest.Write(id[:])
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// seedLoadstandRouteWireTable is called only by the test FullNode constructor,
// before Service.Run. Index zero is the synthetic transit next hop.
func seedLoadstandRouteWireTable(table *routing.Table, own domain.PeerIdentity, count int) error {
	if table == nil || own.IsZero() || string(own[:16]) == loadstandRouteWireSeedPrefix ||
		count <= 0 || count > domain.RouteWireFullMaxDestinations {
		return errRouteWireIncomplete
	}
	transit := loadstandRouteWireSeedIdentity(0)
	for i := 1; i <= count; i++ {
		status, err := table.UpdateRoute(routing.RouteEntry{
			Identity: loadstandRouteWireSeedIdentity(uint32(i)), Origin: transit, NextHop: transit,
			Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		})
		if err != nil || status != routing.RouteAccepted {
			return fmt.Errorf("%w: fixture route %d was not accepted (%v, %v)", errRouteWireIncomplete, i, status, err)
		}
	}
	return nil
}
