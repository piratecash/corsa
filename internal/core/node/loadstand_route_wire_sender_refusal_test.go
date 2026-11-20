package node

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Different original sender refusals are deliberately compared without a new
// observer code or a replacement sender. This proves mechanisms, not the cause
// of any saved native attempt. Each phase uses a fresh original fixture.
func TestLoadStandRouteWireSenderRefusalMechanisms(t *testing.T) {
	for _, name := range []string{"valid", "missing-session", "missing-v3", "disconnected", "full-queue", "oversize-last", "marshal-last"} {
		t.Run(name, func(t *testing.T) {
			for _, phase := range []struct {
				name string
				at   time.Duration
			}{{"before", 9}, {"inside", 12}, {"after", 21}} {
				t.Run(phase.name, func(t *testing.T) {
					s, o, now, queue, entries := fullSendMechanismFixture(t)
					*now = phase.at
					const address domain.PeerAddress = "full-send-mechanism-peer"
					switch name {
					case "missing-session":
						delete(s.sessions, address)
					case "missing-v3":
						s.sessions[address].capabilities = []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1}
					case "disconnected":
						s.health[address].Connected = false
					case "full-queue":
						for len(queue) < cap(queue) {
							queue <- legacyPeerSendItem(protocol.Frame{Type: "sentinel", RawLine: "sentinel\n"})
						}
					case "oversize-last":
						raw, err := json.Marshal(map[string]string{"padding": strings.Repeat("x", protocol.MaxFrameLine+1)})
						if err != nil {
							t.Fatal(err)
						}
						entries[len(entries)-1].Extra = raw
					case "marshal-last":
						entries[len(entries)-1].Extra = json.RawMessage("{")
					}
					if name == "oversize-last" || name == "marshal-last" {
						chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 7, protocol.MaxFrameLine)
						count := 0
						for _, chunk := range chunks {
							count += len(chunk)
						}
						if len(skipped) != 1 || skipped[0] != len(entries)-1 || count != len(entries)-1 {
							t.Fatal("original chunker precondition", skipped, count)
						}
					}
					sent := s.SendRouteAnnounceV3Full(t.Context(), address, 7, entries, domain.RouteWireFullConnect)
					if sent != (name == "valid") {
						t.Fatal("original sender result", sent, name)
					}
					wantIncomplete := name != "valid" && phase.name == "inside" && name != "missing-session" && name != "missing-v3" && name != "disconnected" && name != "full-queue"
					if o.incomplete != wantIncomplete || o.fullPassesPending != 0 {
						t.Fatal("original observer window/refusal changed", o.incomplete, o.fullPassesPending)
					}
					wantEntries := 0
					switch name {
					case "valid":
						wantEntries = len(entries)
					case "oversize-last", "marshal-last":
						wantEntries = len(entries) - 1
					}
					if name == "full-queue" {
						if len(queue) != cap(queue) {
							t.Fatal("refusal consumed a queue slot")
						}
						for len(queue) > 0 {
							if (<-queue).RawLine != "sentinel\n" {
								t.Fatal("sender replaced an existing queue item")
							}
						}
					} else {
						got := 0
						for len(queue) > 0 {
							frame, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte((<-queue).RawLine))
							if err != nil || frame.Kind != protocol.RouteAnnounceV3KindFull || frame.Epoch != 7 {
								t.Fatal("original queued frame", err)
							}
							got += len(frame.Entries)
						}
						if got != wantEntries {
							t.Fatal("original queued entries", got, wantEntries)
						}
					}
					*now = 21
					err := o.captureFullReceipts(nil)
					if wantIncomplete {
						if !errors.Is(err, errRouteWireIncomplete) {
							t.Fatal("original sticky refusal lost", err)
						}
					} else if err != nil {
						t.Fatal("outside-window/control capture changed", err)
					}
				})
			}
		})
	}
}
