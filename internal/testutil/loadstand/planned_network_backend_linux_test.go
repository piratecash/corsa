//go:build linux

package loadstand

import (
	"encoding/json"
	"net/netip"
	"reflect"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPlannedLinkAttributeBoundaryRejectsAmbiguousNames(t *testing.T) {
	valid := append(plannedStringAttribute(unix.IFLA_IFNAME, "ownhost"), plannedStringAttribute(unix.IFLA_IFALIAS, "ownnonce")...)
	name, alias, err := plannedLinkAttributes(valid)
	if err != nil || name != "ownhost" || alias != "ownnonce" {
		t.Fatal("exactlink attributes", name, alias, err)
	}
	for _, bad := range [][]byte{valid[:len(valid)-1], append(append([]byte(nil), valid...), plannedStringAttribute(unix.IFLA_IFNAME, "foreign")...), append(plannedStringAttribute(unix.IFLA_IFALIAS, ""), plannedStringAttribute(unix.IFLA_IFALIAS, "")...), plannedStringAttribute(unix.IFLA_IFNAME, "own\x00foreign")} {
		if _, _, err := plannedLinkAttributes(bad); err == nil {
			t.Fatal("ambiguous/truncated attributes admitted")
		}
	}
}
func TestPlannedNFTExactMemberPairRulesAndCounterProjection(t *testing.T) {
	p := plannedTestPlan()
	objects := plannedNFTObjects(p)
	if len(objects) != 5 {
		t.Fatal("expectedtwo tuple-bound protocol rules plusone blockedcounter", len(objects))
	}
	table := objects[0]["table"].(map[string]any)
	chain := objects[1]["chain"].(map[string]any)
	if table["comment"] != plannedNetworkAlias(p.nonce) || chain["policy"] != "drop" {
		t.Fatal("nonce/policy changed")
	}
	body, err := json.Marshal(map[string]any{"nftables": objects})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := plannedNFTJSON(body)
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i < len(decoded); i++ {
		rule := decoded[i]["rule"].(map[string]any)
		rule["handle"] = json.Number("17")
		expressions := rule["expr"].([]any)
		for _, raw := range expressions {
			expression := raw.(map[string]any)
			if _, ok := expression["counter"]; ok {
				if i == len(decoded)-1 {
					expression["counter"] = map[string]any{"bytes": json.Number("0"), "packets": json.Number("0")}
				} else {
					expression["counter"] = map[string]any{"bytes": json.Number("987"), "packets": json.Number("3")}
				}
			}
		}
	}
	if !plannedNFTBlockedCountersClear(decoded) {
		t.Fatal("zero blocked counter refused")
	}
	lastCounter := decoded[len(decoded)-1]["rule"].(map[string]any)["expr"].([]any)[0].(map[string]any)["counter"].(map[string]any)
	lastCounter["packets"] = json.Number("1")
	if plannedNFTBlockedCountersClear(decoded) {
		t.Fatal("blocked packet evidence hidden")
	}
	evidence := &plannedNFTBlockedEvidence{}
	nft := linuxPlannedNFT{blocked: evidence}
	if !nft.retainBlockedEvidence(decoded) || !evidence.defective || evidence.packets != 1 {
		t.Fatal("defect evidence not retained before own cleanup")
	}
	cleanupProjection, err := normalizePlannedNFT(decoded)
	expectedProjection, expectedErr := normalizePlannedNFT(objects)
	if err != nil || expectedErr != nil || !reflect.DeepEqual(cleanupProjection, expectedProjection) {
		t.Fatal("exact own table became impossible to clean after defect", err, expectedErr)
	}
	lastCounter["packets"] = json.Number("0")
	if !nft.retainBlockedEvidence(decoded) || !evidence.defective || evidence.packets != 1 || nft.operationallyClear(decoded) {
		t.Fatal("cleanup observation erased prior defect or revived run")
	}

	lastCounter["bytes"] = json.Number("1")
	if plannedNFTBlockedCountersClear(decoded) {
		t.Fatal("blocked bytes evidence hidden")
	}
	lastCounter["bytes"] = json.Number("0")
	got, err := normalizePlannedNFT(decoded)
	if err != nil {
		t.Fatal(err)
	}
	want, err := normalizePlannedNFT(objects)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("only dynamic counters/handles should differ", err)
	}
	first := decoded[2]["rule"].(map[string]any)["expr"].([]any)
	first[0] = plannedNFTMatch(map[string]any{"meta": map[string]any{"key": "iifname"}}, "foreign")
	wrong, err := normalizePlannedNFT(decoded)
	if err != nil || reflect.DeepEqual(wrong, want) {
		t.Fatal("foreign source rule normalized away", err)
	}
	for _, counter := range []any{map[string]any{"packets": json.Number("-1"), "bytes": json.Number("0")}, map[string]any{"packets": json.Number("0"), "bytes": json.Number("0"), "foreign": true}} {
		if _, err := normalizePlannedNFT(map[string]any{"counter": counter}); err == nil {
			t.Fatal("invalid counter accepted")
		}
	}
}
func TestPlannedBackendAttemptRowsAreUniqueBeforeKernelEffects(t *testing.T) {
	p := plannedTestPlan()
	b := &linuxPlannedNetworkBackend{plan: p}
	bridge, err := b.begin(plannedNetworkOperation{kind: plannedBridge, plan: p})
	if err != nil || len(b.resources) != 1 || bridge.kernelIndex != 0 || bridge.handle.identity == 0 {
		t.Fatal("unresolved create ownership lost")
	}
	if _, err = b.begin(plannedNetworkOperation{kind: plannedBridge, plan: p}); err == nil || len(b.resources) != 1 {
		t.Fatal("duplicate create authorized")
	}
	for _, member := range p.members {
		row, err := b.begin(plannedNetworkOperation{kind: plannedNamespace, plan: p, member: member})
		if err != nil || row.handle.name != string(member.id) || row.handle.identity == bridge.handle.identity {
			t.Fatal("member creation ownership conflated", err)
		}
	}
	if err = b.release(); err != ErrStopUnconfirmed {
		t.Fatal("unresolved attempts releasedbackend", err)
	}
}

func TestPlannedNetlinkLateOwnReplyDoesNotAuthorizeReplay(t *testing.T) {
	for _, seq := range []uint32{1, 2, 3} {
		current, err := plannedNetlinkCurrentSequence(seq, 4)
		if err != nil || current {
			t.Fatal("late own response not skipped", seq, err)
		}
	}
	if current, err := plannedNetlinkCurrentSequence(4, 4); err != nil || !current {
		t.Fatal("exact current response refused", err)
	}
	for _, pair := range [][2]uint32{{0, 4}, {5, 4}, {1, 0}} {
		if _, err := plannedNetlinkCurrentSequence(pair[0], pair[1]); err == nil {
			t.Fatal("foreign/future response admitted", pair)
		}
	}
	// This is only the exact sequence decision. Actual cancellation after a
	// kernel mutation and late ACK cleanup still require Linux native tests.
}

func TestPlannedNFTKnownNoncreationCannotAdoptPriorTable(t *testing.T) {
	p := plannedTestPlan()
	b := &linuxPlannedNetworkBackend{plan: p}
	row, err := b.begin(plannedNetworkOperation{kind: plannedFirewall, plan: p})
	if err != nil {
		t.Fatal(err)
	}
	// This is the exact state after nft preflight refuses an existingtable:
	// no process/kernel create was attempted. Empty tool cannot run, so any
	// attempt to inspect/adopt/delete the old matching table fails this control.
	if row.nftAttempted || row.nftHandle != 0 {
		t.Fatal("known noncreation misclassified")
	}
	if err = b.remove(t.Context(), row.handle); err != nil || !row.removed {
		t.Fatal("noncreation tried to acquire prior table", err)
	}
}

// This finite evaluator is only an oracle for our emitted JSON expression
// subset. It does not execute libnftables or prove kernel/readback conformance.
type plannedNFTTestPacket struct{ ether, input, output, source, target string }

func plannedNFTTestValue(t *testing.T, raw any, packet plannedNFTTestPacket) any {
	t.Helper()
	if text, ok := raw.(string); ok {
		return text
	}
	object, ok := raw.(map[string]any)
	if !ok || len(object) != 1 {
		t.Fatal("unexpected expression", raw)
		return nil
	}
	if raw, ok := object["concat"]; ok {
		parts := raw.([]any)
		values := make([]any, len(parts))
		for i, part := range parts {
			values[i] = plannedNFTTestValue(t, part, packet)
		}
		return values
	}
	if raw, ok := object["meta"]; ok {
		switch raw.(map[string]any)["key"] {
		case "iifname":
			return packet.input
		case "oifname":
			return packet.output
		}
		t.Fatal("unexpected meta")
		return nil
	}
	if raw, ok := object["payload"]; ok {
		field := raw.(map[string]any)
		protocol, _ := field["protocol"].(string)
		name, _ := field["field"].(string)
		switch protocol + "/" + name {
		case "ether/type":
			return packet.ether
		case "ip/saddr", "arp/saddr ip":
			return packet.source
		case "ip/daddr", "arp/daddr ip":
			return packet.target
		}
		t.Fatal("unexpected payload")
		return nil
	}
	t.Fatal("unexpected expression key")
	return nil
}
func plannedNFTTestAccepts(t *testing.T, objects []map[string]any, packet plannedNFTTestPacket) bool {
	t.Helper()
	for _, object := range objects {
		rule, ok := object["rule"].(map[string]any)
		if !ok {
			continue
		}
		matched := true
		for _, raw := range rule["expr"].([]any) {
			statement := raw.(map[string]any)
			if len(statement) != 1 {
				t.Fatal("unexpected statement")
			}
			if raw, ok := statement["match"]; ok {
				match := raw.(map[string]any)
				left := plannedNFTTestValue(t, match["left"], packet)
				equal := false
				if object, ok := match["right"].(map[string]any); ok {
					values, ok := object["set"].([]any)
					if !ok || len(object) != 1 {
						t.Fatal("unexpected set")
					}
					for _, value := range values {
						if reflect.DeepEqual(left, plannedNFTTestValue(t, value, packet)) {
							equal = true
						}
					}
				} else {
					equal = reflect.DeepEqual(left, plannedNFTTestValue(t, match["right"], packet))
				}
				switch match["op"] {
				case "==":
					matched = equal
				case "!=":
					matched = !equal
				default:
					t.Fatal("unexpected comparison")
				}
				if !matched {
					break
				}
				continue
			}
			if _, ok := statement["counter"]; ok {
				continue
			}
			if _, ok := statement["accept"]; ok {
				return matched
			}
			if _, ok := statement["drop"]; ok {
				return false
			}
			t.Fatal("unexpected action")
		}
	}
	return false
}
func plannedNFTTestGraph(p plannedNetworkPlan, packet plannedNFTTestPacket) bool {
	if packet.input == packet.output {
		return false
	}
	sourceOK, outputOK, targetOK, exactDestination := false, false, false, false
	for _, member := range p.members {
		sourceOK = sourceOK || packet.input == member.hostLink && packet.source == member.address.String()
		outputOK = outputOK || packet.output == member.hostLink
		targetOK = targetOK || packet.target == member.address.String()
		exactDestination = exactDestination || packet.output == member.hostLink && packet.target == member.address.String()
	}
	switch packet.ether {
	case "ip":
		return sourceOK && exactDestination
	case "arp":
		return sourceOK && outputOK && targetOK
	}
	return false
}
func plannedNFTTestThreeMembers() plannedNetworkPlan {
	p := plannedTestPlan()
	third := p.members[1]
	third.id = "third"
	third.hostLink = "h2"
	third.peerLink = "p2"
	third.address = netip.MustParseAddr("240.2.0.1")
	p.members = append(p.members, third)
	return p
}
func TestPlannedARPBroadcastTargetsStayWithinOriginalMemberSet(t *testing.T) {
	p := plannedNFTTestThreeMembers()
	objects := plannedNFTObjects(p)
	ports, addresses := []string{"foreign"}, []string{"198.51.100.1"}
	for _, member := range p.members {
		ports = append(ports, member.hostLink)
		addresses = append(addresses, member.address.String())
	}
	for _, ether := range []string{"ip", "arp", "ip6"} {
		for _, input := range ports {
			for _, output := range ports {
				for _, source := range addresses {
					for _, target := range addresses {
						packet := plannedNFTTestPacket{ether, input, output, source, target}
						got, want := plannedNFTTestAccepts(t, objects, packet), plannedNFTTestGraph(p, packet)
						if got != want {
							t.Fatal("tuple policy changed exact original graph", packet, got, want)
						}
					}
				}
			}
		}
	}
}
func TestPlannedNFTTupleLookupsRejectIndependentMembership(t *testing.T) {
	p := plannedNFTTestThreeMembers()
	clone := func() []map[string]any {
		raw, err := json.Marshal(map[string]any{"nftables": plannedNFTObjects(p)})
		if err != nil {
			t.Fatal(err)
		}
		objects, err := plannedNFTJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		return objects
	}
	// Each directed corruption must actually admit its forbidden packet. This
	// keeps the independent graph oracle sensitive to tuple/diagonal/target loss.
	objects := clone()
	expr := objects[2]["rule"].(map[string]any)["expr"].([]any)
	expr[3] = map[string]any{"counter": nil}
	packet := plannedNFTTestPacket{"ip", p.members[0].hostLink, p.members[0].hostLink, p.members[0].address.String(), p.members[0].address.String()}
	if !plannedNFTTestAccepts(t, objects, packet) || plannedNFTTestGraph(p, packet) {
		t.Fatal("same-member negative control blind")
	}
	objects = clone()
	expr = objects[2]["rule"].(map[string]any)["expr"].([]any)
	expr[2] = map[string]any{"counter": nil}
	packet = plannedNFTTestPacket{"ip", p.members[0].hostLink, p.members[1].hostLink, p.members[0].address.String(), p.members[2].address.String()}
	if !plannedNFTTestAccepts(t, objects, packet) || plannedNFTTestGraph(p, packet) {
		t.Fatal("destination-tuple negative control blind")
	}
	objects = clone()
	expr = objects[3]["rule"].(map[string]any)["expr"].([]any)
	match := expr[3].(map[string]any)["match"].(map[string]any)
	set := match["right"].(map[string]any)
	set["set"] = append(set["set"].([]any), "198.51.100.1")
	packet = plannedNFTTestPacket{"arp", p.members[0].hostLink, p.members[1].hostLink, p.members[0].address.String(), "198.51.100.1"}
	if !plannedNFTTestAccepts(t, objects, packet) || plannedNFTTestGraph(p, packet) {
		t.Fatal("foreign-target negative control blind")
	}
}
func TestPlannedNFTRuleEncodingGrowsLinearly(t *testing.T) {
	var countSets func(any) int
	countSets = func(raw any) int {
		total := 0
		switch object := raw.(type) {
		case map[string]any:
			for key, value := range object {
				if key == "set" {
					total += len(value.([]any))
				} else {
					total += countSets(value)
				}
			}
		case []any:
			for _, value := range object {
				total += countSets(value)
			}
		case []map[string]any:
			for _, value := range object {
				total += countSets(value)
			}
		}
		return total
	}
	for _, size := range []int{1, 16, 256, 1000} {
		p := plannedTestPlan()
		template := p.members[0]
		p.members = nil
		for i := 0; i < size; i++ {
			member := template
			member.id = MemberID("member" + strconv.Itoa(i))
			member.hostLink = "h" + strconv.Itoa(i)
			member.address = netip.AddrFrom4([4]byte{240 + byte(i>>8), byte(i), 0, 1})
			p.members = append(p.members, member)
		}
		objects := plannedNFTObjects(p)
		raw, err := json.Marshal(objects)
		if err != nil {
			t.Fatal(err)
		}
		if len(objects) != 5 || countSets(objects) != 7*size || len(raw) > 4000+size*800 {
			t.Fatal("rule encoding expanded beyond linear original-member storage", size, len(objects), countSets(objects), len(raw))
		}
	}
}
