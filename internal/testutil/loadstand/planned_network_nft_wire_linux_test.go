//go:build linux

package loadstand

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// The JSON literal below is the actual 2480-byte readback from the retained
// failed VM. ALL binary netlink fixtures in this file are CONSTRUCTED, not
// claimed actual kernel evidence. The original VM must verify the real wire.
const plannedNFTActualElidedJSON = `{"nftables": [{"metainfo": {"version": "1.1.6", "release_name": "Commodore Bullmoose #7", "json_schema_version": 1}}, {"table": {"family": "bridge", "name": "br86cd0851", "handle": 1, "comment": "corsa.loadstand:86cd0851ba946b4bd0fa1f1c8c246c3ff9dfebdc82db6b2abbb560a4149a7c34"}}, {"chain": {"family": "bridge", "table": "br86cd0851", "name": "forward", "handle": 1, "type": "filter", "hook": "forward", "prio": 0, "policy": "drop"}}, {"rule": {"family": "bridge", "table": "br86cd0851", "chain": "forward", "handle": 5, "expr": [{"match": {"op": "==", "left": {"concat": [{"meta": {"key": "iifname"}}, {"payload": {"protocol": "ip", "field": "saddr"}}]}, "right": {"set": [{"concat": ["h0", "240.0.0.1"]}, {"concat": ["h1", "240.1.0.1"]}, {"concat": ["h2", "240.2.0.1"]}, {"concat": ["h3", "240.3.0.1"]}]}}}, {"match": {"op": "==", "left": {"concat": [{"meta": {"key": "oifname"}}, {"payload": {"protocol": "ip", "field": "daddr"}}]}, "right": {"set": [{"concat": ["h0", "240.0.0.1"]}, {"concat": ["h1", "240.1.0.1"]}, {"concat": ["h2", "240.2.0.1"]}, {"concat": ["h3", "240.3.0.1"]}]}}}, {"match": {"op": "!=", "left": {"concat": [{"meta": {"key": "iifname"}}, {"meta": {"key": "oifname"}}]}, "right": {"set": [{"concat": ["h0", "h0"]}, {"concat": ["h1", "h1"]}, {"concat": ["h2", "h2"]}, {"concat": ["h3", "h3"]}]}}}, {"counter": {"packets": 0, "bytes": 0}}, {"accept": null}]}}, {"rule": {"family": "bridge", "table": "br86cd0851", "chain": "forward", "handle": 10, "expr": [{"match": {"op": "==", "left": {"concat": [{"meta": {"key": "iifname"}}, {"payload": {"protocol": "arp", "field": "saddr ip"}}]}, "right": {"set": [{"concat": ["h0", "240.0.0.1"]}, {"concat": ["h1", "240.1.0.1"]}, {"concat": ["h2", "240.2.0.1"]}, {"concat": ["h3", "240.3.0.1"]}]}}}, {"match": {"op": "==", "left": {"meta": {"key": "oifname"}}, "right": {"set": ["h0", "h1", "h2", "h3"]}}}, {"match": {"op": "==", "left": {"payload": {"protocol": "arp", "field": "daddr ip"}}, "right": {"set": ["240.0.0.1", "240.1.0.1", "240.2.0.1", "240.3.0.1"]}}}, {"match": {"op": "!=", "left": {"concat": [{"meta": {"key": "iifname"}}, {"meta": {"key": "oifname"}}]}, "right": {"set": [{"concat": ["h0", "h0"]}, {"concat": ["h1", "h1"]}, {"concat": ["h2", "h2"]}, {"concat": ["h3", "h3"]}]}}}, {"counter": {"packets": 0, "bytes": 0}}, {"accept": null}]}}, {"rule": {"family": "bridge", "table": "br86cd0851", "chain": "forward", "handle": 11, "expr": [{"counter": {"packets": 0, "bytes": 0}}, {"drop": null}]}}]}
`

func nftWireFixtureU32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
func nftWireFixtureU64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
func nftWireFixtureAttr(kind uint16, data []byte) []byte {
	b := make([]byte, (len(data)+7)&^3)
	binary.NativeEndian.PutUint16(b, uint16(4+len(data)))
	binary.NativeEndian.PutUint16(b[2:], kind)
	copy(b[4:], data)
	return b
}
func nftWireFixtureString(kind uint16, value string) []byte {
	return nftWireFixtureAttr(kind, append([]byte(value), 0))
}
func nftWireFixtureExpr(name string, attrs ...[]byte) []byte {
	var data []byte
	for _, a := range attrs {
		data = append(data, a...)
	}
	return nftWireFixtureAttr(1, append(nftWireFixtureString(1, name), nftWireFixtureAttr(2, data)...))
}
func nftWireFixturePayload(reg, base, offset, length uint32) []byte {
	return nftWireFixtureExpr("payload", nftWireFixtureAttr(1, nftWireFixtureU32(reg)), nftWireFixtureAttr(2, nftWireFixtureU32(base)), nftWireFixtureAttr(3, nftWireFixtureU32(offset)), nftWireFixtureAttr(4, nftWireFixtureU32(length)))
}
func nftWireFixtureCmp(reg, op uint32, value []byte) []byte {
	return nftWireFixtureExpr("cmp", nftWireFixtureAttr(1, nftWireFixtureU32(reg)), nftWireFixtureAttr(2, nftWireFixtureU32(op)), nftWireFixtureAttr(3, nftWireFixtureAttr(1, value)))
}
func nftWireFixtureMeta(key, reg uint32) []byte {
	return nftWireFixtureExpr("meta", nftWireFixtureAttr(1, nftWireFixtureU32(reg)), nftWireFixtureAttr(2, nftWireFixtureU32(key)))
}
func nftWireFixtureLookup(name string) []byte {
	return nftWireFixtureExpr("lookup", nftWireFixtureString(1, name), nftWireFixtureAttr(2, nftWireFixtureU32(1)))
}
func nftWireFixtureTail(verdict uint32) []byte {
	counter := nftWireFixtureExpr("counter", nftWireFixtureAttr(1, nftWireFixtureU64(0)), nftWireFixtureAttr(2, nftWireFixtureU64(0)))
	end := nftWireFixtureExpr("immediate", nftWireFixtureAttr(1, nftWireFixtureU32(0)), nftWireFixtureAttr(2, nftWireFixtureAttr(2, nftWireFixtureAttr(1, nftWireFixtureU32(verdict)))))
	return append(counter, end...)
}
func nftWireFixturePlan(t *testing.T) plannedNetworkPlan {
	t.Helper()
	p := plannedTestPlan()
	b, err := hex.DecodeString("86cd0851ba946b4bd0fa1f1c8c246c3ff9dfebdc82db6b2abbb560a4149a7c34")
	if err != nil {
		t.Fatal(err)
	}
	copy(p.nonce[:], b)
	p.bridge = "br86cd0851"
	p.members = nil
	for i := 0; i < 4; i++ {
		p.members = append(p.members, plannedNetworkMember{id: MemberID(fmt.Sprintf("full-%03d", i)), hostLink: fmt.Sprintf("h%d", i), peerLink: fmt.Sprintf("p%d", i), address: netip.MustParseAddr(fmt.Sprintf("240.%d.0.1", i))})
	}
	return p
}
func nftWireFixtureObjects(t *testing.T) []map[string]any {
	t.Helper()
	objects, err := plannedNFTJSON([]byte(plannedNFTActualElidedJSON))
	if err != nil {
		t.Fatal(err)
	}
	return objects[1:]
}
func nftWireFixtureRule(table string, handle, previous uint64, gen uint32, expressions []byte) syscall.NetlinkMessage {
	body := []byte{unix.AF_BRIDGE, 0, byte(gen >> 8), byte(gen)}
	body = append(body, nftWireFixtureString(1, table)...)
	body = append(body, nftWireFixtureString(2, "forward")...)
	body = append(body, nftWireFixtureAttr(3, nftWireFixtureU64(handle))...)
	if previous != 0 {
		body = append(body, nftWireFixtureAttr(6, nftWireFixtureU64(previous))...)
	}
	body = append(body, nftWireFixtureAttr(4, expressions)...)
	return syscall.NetlinkMessage{Header: syscall.NlMsghdr{Type: plannedNFTNewRule, Flags: unix.NLM_F_MULTI | unix.NLM_F_APPEND, Seq: 9, Pid: 77}, Data: body}
}
func nftWireFixtureRules(p plannedNetworkPlan) []syscall.NetlinkMessage {
	// Structural primitive fixtures for the same three predicates, with synthetic
	// anonymous-set names. Only the required guard witness is asserted byte-exact.
	ip := append(nftWireFixturePayload(1, 0, 12, 2), nftWireFixtureCmp(1, 0, []byte{8, 0})...)
	ip = append(ip, nftWireFixtureMeta(6, 1)...)
	ip = append(ip, nftWireFixturePayload(12, 1, 12, 4)...)
	ip = append(ip, nftWireFixtureLookup("__tuple_source")...)
	ip = append(ip, nftWireFixtureMeta(7, 1)...)
	ip = append(ip, nftWireFixturePayload(12, 1, 16, 4)...)
	ip = append(ip, nftWireFixtureLookup("__tuple_target")...)
	ip = append(ip, nftWireFixtureMeta(6, 1)...)
	ip = append(ip, nftWireFixtureMeta(7, 2)...)
	ip = append(ip, nftWireFixtureLookup("__diagonal")...)
	ip = append(ip, nftWireFixtureTail(1)...)
	arp := append(nftWireFixturePayload(1, 0, 12, 2), nftWireFixtureCmp(1, 0, []byte{8, 6})...)
	arp = append(arp, nftWireFixtureMeta(6, 1)...)
	arp = append(arp, nftWireFixturePayload(12, 1, 14, 4)...)
	arp = append(arp, nftWireFixtureLookup("__arp_source")...)
	arp = append(arp, nftWireFixtureMeta(7, 1)...)
	arp = append(arp, nftWireFixtureLookup("__ports")...)
	arp = append(arp, nftWireFixturePayload(1, 1, 24, 4)...)
	arp = append(arp, nftWireFixtureLookup("__addresses")...)
	arp = append(arp, nftWireFixtureMeta(6, 1)...)
	arp = append(arp, nftWireFixtureMeta(7, 2)...)
	arp = append(arp, nftWireFixtureLookup("__diagonal")...)
	arp = append(arp, nftWireFixtureTail(1)...)
	return []syscall.NetlinkMessage{nftWireFixtureRule(p.bridge, 5, 0, 42, ip), nftWireFixtureRule(p.bridge, 10, 5, 42, arp), nftWireFixtureRule(p.bridge, 11, 10, 42, nftWireFixtureTail(0))}
}

func TestPlannedNFTRawGuardJoinsCapturedJSON(t *testing.T) {
	p := nftWireFixturePlan(t)
	actual := nftWireFixtureObjects(t)
	rows := nftWireFixtureRules(p)
	t.Run("actual-elided-json-with-constructed-wire", func(t *testing.T) {
		if err := plannedNFTExactReadback(p, actual, rows, 42, 42); err != nil {
			t.Fatal("exact EtherType witness failed captured readback", err)
		}
	})
	t.Run("explicit-producer-with-same-witness", func(t *testing.T) {
		raw, err := json.Marshal(map[string]any{"nftables": plannedNFTObjects(p)})
		if err != nil {
			t.Fatal(err)
		}
		objects, err := plannedNFTJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		for i, h := range []int{5, 10, 11} {
			objects[2+i]["rule"].(map[string]any)["handle"] = json.Number(fmt.Sprint(h))
		}
		if err := plannedNFTExactReadback(p, objects, rows, 42, 42); err != nil {
			t.Fatal("explicit guard form refused", err)
		}
	})
	t.Run("empty-plan-exact-drop", func(t *testing.T) {
		p := p
		p.members = nil
		raw, _ := json.Marshal(map[string]any{"nftables": plannedNFTObjects(p)})
		objects, err := plannedNFTJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		objects[2]["rule"].(map[string]any)["handle"] = json.Number("5")
		one := []syscall.NetlinkMessage{nftWireFixtureRule(p.bridge, 5, 0, 42, nftWireFixtureTail(0))}
		if err = plannedNFTExactReadback(p, objects, one, 42, 42); err != nil {
			t.Fatal("empty original drop rule refused", err)
		}
	})
	t.Run("producer-retains-both-ether-guards", func(t *testing.T) {
		objects := plannedNFTObjects(p)
		for i, protocol := range []string{"ip", "arp"} {
			expr := objects[2+i]["rule"].(map[string]any)["expr"].([]any)
			if !reflect.DeepEqual(expr[0], plannedNFTMatch(plannedNFTPayload("ether", "type"), protocol)) {
				t.Fatal("creation EtherType policy changed")
			}
		}
	})
}
func TestPlannedNFTRawGuardRejectsSameJSONAliases(t *testing.T) {
	for _, name := range []string{"cmp-len1-padded-collision", "meta-protocol-vlan", "wrong-register", "wrong-base", "wrong-offset", "wrong-load-length", "wrong-op", "wrong-ether-value", "guard-order", "extra-payload-attribute"} {
		t.Run(name, func(t *testing.T) {
			p := nftWireFixturePlan(t)
			actual := nftWireFixtureObjects(t)
			rows := nftWireFixtureRules(p)
			load := nftWireFixturePayload(1, 0, 12, 2)
			cmp := nftWireFixtureCmp(1, 0, []byte{8, 0})
			switch name {
			case "cmp-len1-padded-collision":
				cmp = nftWireFixtureCmp(1, 0, []byte{8})
			case "meta-protocol-vlan":
				load = nftWireFixtureMeta(1, 1)
			case "wrong-register":
				cmp = nftWireFixtureCmp(2, 0, []byte{8, 0})
			case "wrong-base":
				load = nftWireFixturePayload(1, 1, 12, 2)
			case "wrong-offset":
				load = nftWireFixturePayload(1, 0, 13, 2)
			case "wrong-load-length":
				load = nftWireFixturePayload(1, 0, 12, 1)
			case "wrong-op":
				cmp = nftWireFixtureCmp(1, 1, []byte{8, 0})
			case "wrong-ether-value":
				cmp = nftWireFixtureCmp(1, 0, []byte{8, 6})
			case "guard-order":
				load, cmp = cmp, load
			case "extra-payload-attribute":
				load = nftWireFixtureExpr("payload", nftWireFixtureAttr(1, nftWireFixtureU32(1)), nftWireFixtureAttr(2, nftWireFixtureU32(0)), nftWireFixtureAttr(3, nftWireFixtureU32(12)), nftWireFixtureAttr(4, nftWireFixtureU32(2)), nftWireFixtureAttr(5, nftWireFixtureU32(1)))
			}
			rows[0] = nftWireFixtureRule(p.bridge, 5, 0, 42, append(append(load, cmp...), nftWireFixtureTail(1)...))
			if err := plannedNFTExactReadback(p, actual, rows, 42, 42); err == nil {
				t.Fatal("same printable JSON admitted non-original EtherType guard")
			}
		})
	}
}
func TestPlannedNFTRawGuardRejectsMixedOrChangedCuts(t *testing.T) {
	for _, name := range []string{"generation", "low-generation", "wrong-handle", "rule-order", "missing-rule", "extra-rule", "unknown-rule-attribute", "wrong-family", "version", "previous-handle", "unknown-expression", "truncated", "duplicate-rule-attribute", "duplicate-expression-attribute", "nonzero-attribute-padding", "changed-payload", "changed-verdict", "extra-json-object", "reordered-json"} {
		t.Run(name, func(t *testing.T) {
			p := nftWireFixturePlan(t)
			actual := nftWireFixtureObjects(t)
			rows := nftWireFixtureRules(p)
			after := uint32(42)
			switch name {
			case "generation":
				after = 43
			case "low-generation":
				rows[0].Data[3] = 41
			case "wrong-handle":
				rows[0] = nftWireFixtureRule(p.bridge, 6, 0, 42, append(append(nftWireFixturePayload(1, 0, 12, 2), nftWireFixtureCmp(1, 0, []byte{8, 0})...), nftWireFixtureTail(1)...))
			case "rule-order":
				rows[0], rows[1] = rows[1], rows[0]
			case "missing-rule":
				rows = rows[:2]
			case "extra-rule":
				rows = append(rows, rows[2])
			case "unknown-rule-attribute":
				rows[0].Data = append(rows[0].Data, nftWireFixtureAttr(31, nil)...)
			case "wrong-family":
				rows[0].Data[0] = unix.AF_INET
			case "version":
				rows[0].Data[1] = 1
			case "previous-handle":
				rows[1] = nftWireFixtureRule(p.bridge, 10, 6, 42, nftWireFixtureTail(1))
			case "unknown-expression":
				rows[0] = nftWireFixtureRule(p.bridge, 5, 0, 42, append(nftWireFixtureExpr("foreign", nil), nftWireFixtureTail(1)...))
			case "truncated":
				rows[0].Data = rows[0].Data[:len(rows[0].Data)-1]
			case "duplicate-rule-attribute":
				rows[0].Data = append(rows[0].Data, nftWireFixtureString(1, p.bridge)...)
			case "duplicate-expression-attribute":
				attributes, err := plannedNFTAttributes(rows[0].Data[4:], map[uint16]bool{1: false, 2: false, 3: false, 4: true, 6: false, 8: false}, 8)
				if err != nil {
					t.Fatal("valid rule fixture refused before duplicate negative", err)
				}
				expressions := attributes[4]
				firstLength := int(binary.NativeEndian.Uint16(expressions))
				first := expressions[:plannedAlign(firstLength)]
				name := nftWireFixtureString(1, "payload")
				// Insert one duplicate NAME into the otherwise exact first
				// payload; retain its valid DATA and every following expression.
				data := append(append([]byte(nil), first[4:4+len(name)]...), name...)
				data = append(data, first[4+len(name):firstLength]...)
				duplicate := nftWireFixtureAttr(1, data)
				rows[0] = nftWireFixtureRule(p.bridge, 5, 0, 42, append(duplicate, expressions[plannedAlign(firstLength):]...))
			case "nonzero-attribute-padding":
				tableAttributeLength := 4 + len(p.bridge) + 1
				if tableAttributeLength%4 == 0 {
					t.Fatal("padding-negative fixture has no table padding")
				}
				rows[0].Data[4+tableAttributeLength] = 1
			case "changed-payload":
				expr := actual[2]["rule"].(map[string]any)["expr"].([]any)
				expr[0] = plannedNFTMatch(plannedNFTPayload("ip", "daddr"), "240.0.0.1")
			case "changed-verdict":
				expr := actual[4]["rule"].(map[string]any)["expr"].([]any)
				expr[1] = map[string]any{"accept": nil}
			case "extra-json-object":
				actual = append(actual, map[string]any{"foreign": true})
			case "reordered-json":
				actual[2], actual[3] = actual[3], actual[2]
			}
			if err := plannedNFTExactReadback(p, actual, rows, 42, after); err == nil {
				t.Fatal("mixed or changed exact policy admitted")
			}
		})
	}
}
func nftWireFixtureGeneration() syscall.NetlinkMessage {
	data := []byte{unix.AF_UNSPEC, 0, 0, 42}
	data = append(data, nftWireFixtureAttr(1, nftWireFixtureU32(42))...)
	data = append(data, nftWireFixtureAttr(2, nftWireFixtureU32(501))...)
	data = append(data, nftWireFixtureString(3, "node.test")...)
	return syscall.NetlinkMessage{Header: syscall.NlMsghdr{Type: plannedNFTNewGen, Seq: 9, Pid: 77}, Data: data}
}
func TestPlannedNFTWireRepliesAreClosedAndComplete(t *testing.T) {
	t.Run("typed-generation-only", func(t *testing.T) {
		r := plannedNFTReplies{seq: 9, port: 77, kind: plannedNFTNewGen, limit: 1}
		m := nftWireFixtureGeneration()
		if err := r.accept(m); err != nil || !r.complete() {
			t.Fatal("typed generation refused", err)
		}
		if id, err := plannedNFTGeneration(m); err != nil || id != 42 {
			t.Fatal("typed generation fields refused", id, err)
		}
	})
	t.Run("dump-needs-done", func(t *testing.T) {
		p := nftWireFixturePlan(t)
		r := plannedNFTReplies{seq: 9, port: 77, kind: plannedNFTNewRule, dump: true, limit: 3}
		for _, m := range nftWireFixtureRules(p) {
			if err := r.accept(m); err != nil {
				t.Fatal(err)
			}
		}
		if r.complete() {
			t.Fatal("partial dump counted complete")
		}
		done := syscall.NetlinkMessage{Header: syscall.NlMsghdr{Type: unix.NLMSG_DONE, Flags: unix.NLM_F_MULTI, Seq: 9, Pid: 77}, Data: make([]byte, 4)}
		if err := r.accept(done); err != nil || !r.complete() {
			t.Fatal("complete dump refused", err)
		}
		if err := r.accept(done); err == nil {
			t.Fatal("duplicate completion admitted")
		}
	})
	for _, name := range []string{"success-ack-not-generation", "wrong-sequence", "wrong-port", "wrong-type", "dump-interrupted", "overrun", "duplicate-generation", "unknown-generation-attribute", "duplicate-generation-attribute", "generation-res-id", "missing-generation", "generation-short-number", "done-malformed", "done-negative-errno", "done-positive-error", "negative-error-errno"} {
		t.Run(name, func(t *testing.T) {
			r := plannedNFTReplies{seq: 9, port: 77, kind: plannedNFTNewGen, limit: 1, request: make([]byte, unix.NLMSG_HDRLEN)}
			m := nftWireFixtureGeneration()
			schemaOnly := false
			var expectedErr error
			switch name {
			case "success-ack-not-generation":
				m.Header.Type = unix.NLMSG_ERROR
				m.Data = make([]byte, 4+unix.NLMSG_HDRLEN)
			case "wrong-sequence":
				m.Header.Seq++
			case "wrong-port":
				m.Header.Pid = 0
			case "wrong-type":
				m.Header.Type = plannedNFTNewRule
			case "dump-interrupted":
				m.Header.Flags = unix.NLM_F_DUMP_INTR
			case "overrun":
				m.Header.Type = unix.NLMSG_OVERRUN
			case "duplicate-generation":
				if err := r.accept(m); err != nil {
					t.Fatal(err)
				}
			case "unknown-generation-attribute":
				m.Data = append(m.Data, nftWireFixtureAttr(4, nil)...)
				schemaOnly = true
			case "duplicate-generation-attribute":
				m.Data = append(m.Data, nftWireFixtureAttr(1, nftWireFixtureU32(42))...)
				schemaOnly = true
			case "generation-res-id":
				m.Data[3] = 43
				schemaOnly = true
			case "missing-generation":
				m.Data = []byte{0, 0, 0, 42}
				schemaOnly = true
			case "generation-short-number":
				m.Data = append([]byte{0, 0, 0, 42}, nftWireFixtureAttr(1, []byte{42})...)
				schemaOnly = true
			case "done-malformed", "done-negative-errno", "done-positive-error":
				r.dump, r.kind, r.limit = true, plannedNFTNewRule, 3
				for _, row := range nftWireFixtureRules(nftWireFixturePlan(t)) {
					if err := r.accept(row); err != nil {
						t.Fatal("valid rule prefix refused before DONE negative", err)
					}
				}
				m.Header.Type, m.Header.Flags = unix.NLMSG_DONE, unix.NLM_F_MULTI
				m.Data = make([]byte, 4)
				switch name {
				case "done-malformed":
					m.Data = m.Data[:3]
				case "done-negative-errno":
					code := -int32(syscall.EPERM)
					binary.NativeEndian.PutUint32(m.Data, uint32(code))
					expectedErr = syscall.EPERM
				default:
					binary.NativeEndian.PutUint32(m.Data, 1)
				}
			case "negative-error-errno":
				m.Header.Type = unix.NLMSG_ERROR
				m.Data = make([]byte, 4+unix.NLMSG_HDRLEN)
				code := -int32(syscall.EACCES)
				binary.NativeEndian.PutUint32(m.Data, uint32(code))
				expectedErr = syscall.EACCES
			}
			if schemaOnly {
				if _, err := plannedNFTGeneration(m); err == nil {
					t.Fatal("invalid generation evidence admitted")
				}
			} else {
				err := r.accept(m)
				if err == nil {
					t.Fatal("invalid netfilter reply admitted")
				}
				if expectedErr != nil && !errors.Is(err, expectedErr) {
					t.Fatal("kernel reply errno identity lost", err, expectedErr)
				}
				if r.dump && r.complete() {
					t.Fatal("malformed or failed DONE completed the dump")
				}
			}
		})
	}
}
func nftWireFixturePacket(m syscall.NetlinkMessage) []byte {
	b := make([]byte, (unix.NLMSG_HDRLEN+len(m.Data)+3)&^3)
	binary.NativeEndian.PutUint32(b, uint32(unix.NLMSG_HDRLEN+len(m.Data)))
	binary.NativeEndian.PutUint16(b[4:], m.Header.Type)
	binary.NativeEndian.PutUint16(b[6:], m.Header.Flags)
	binary.NativeEndian.PutUint32(b[8:], m.Header.Seq)
	binary.NativeEndian.PutUint32(b[12:], m.Header.Pid)
	copy(b[unix.NLMSG_HDRLEN:], m.Data)
	return b
}
func TestPlannedNFTDatagramRejectsForeignPartialAndOverflow(t *testing.T) {
	t.Run("kernel-sender-requester-header", func(t *testing.T) {
		r := plannedNFTReplies{seq: 9, port: 77, kind: plannedNFTNewGen, limit: 1}
		raw := nftWireFixturePacket(nftWireFixtureGeneration())
		if err := r.datagram(raw, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}, 0); err != nil || !r.complete() {
			t.Fatal("kernel sender / requester destination framing refused", err)
		}
	})
	for _, name := range []string{"nonkernel-sender", "multicast-sender", "wrong-sender-family", "truncated-datagram", "truncated-control", "trailing-byte", "empty", "total-budget", "datagram-budget", "duplicate-same-datagram"} {
		t.Run(name, func(t *testing.T) {
			r := plannedNFTReplies{seq: 9, port: 77, kind: plannedNFTNewGen, limit: 1}
			raw := nftWireFixturePacket(nftWireFixtureGeneration())
			from := &unix.SockaddrNetlink{Family: unix.AF_NETLINK}
			flags := 0
			switch name {
			case "nonkernel-sender":
				from.Pid = 77
			case "multicast-sender":
				from.Groups = 1
			case "wrong-sender-family":
				from.Family = unix.AF_INET
			case "truncated-datagram":
				flags = unix.MSG_TRUNC
			case "truncated-control":
				flags = unix.MSG_CTRUNC
			case "trailing-byte":
				raw = append(raw, 0)
			case "empty":
				raw = nil
			case "total-budget":
				r.bytes = processFileLimit - len(raw) + 1
			case "datagram-budget":
				raw = make([]byte, processFileLimit)
			case "duplicate-same-datagram":
				raw = append(raw, raw...)
			}
			if err := r.datagram(raw, from, flags); err == nil {
				t.Fatal("foreign, partial, duplicate or oversized kernel evidence admitted")
			}
		})
	}
}

// Closed per-call fake owners exercise the SAME verification/removal core as
// the real adapter. No package-global hook or executable bypass is installed.
type nftWireOwnerFixture struct {
	raw                   []byte
	rows                  []syscall.NetlinkMessage
	trace                 []string
	fail                  string
	failure, closeFailure error
	handle                uint64
	exists                bool
	after                 uint32
	generations, closes   int
	deleted               []byte
}

func nftWireFixtureOwner(t *testing.T, p plannedNetworkPlan) *nftWireOwnerFixture {
	t.Helper()
	return &nftWireOwnerFixture{raw: []byte(plannedNFTActualElidedJSON), rows: nftWireFixtureRules(p), failure: errors.New("owned read failure"), closeFailure: errors.New("owned close failure"), handle: 1, exists: true, after: 42}
}
func (f *nftWireOwnerFixture) openWire(ctx context.Context) (plannedNFTReadbackWire, error) {
	f.trace = append(f.trace, "open")
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.fail == "open" {
		return nil, f.failure
	}
	return f, nil
}
func (f *nftWireOwnerFixture) generation(context.Context) (uint32, error) {
	f.trace = append(f.trace, "generation")
	f.generations++
	if f.fail == "before" && f.generations == 1 || f.fail == "after" && f.generations == 2 {
		return 0, f.failure
	}
	if f.generations == 1 {
		return 42, nil
	}
	return f.after, nil
}
func (f *nftWireOwnerFixture) tables(context.Context, string) (uint64, bool, error) {
	f.trace = append(f.trace, "tables")
	if f.fail == "tables" {
		return 0, false, f.failure
	}
	return f.handle, f.exists, nil
}
func (f *nftWireOwnerFixture) list(context.Context, string) ([]byte, error) {
	f.trace = append(f.trace, "json")
	if f.fail == "json" {
		return nil, f.failure
	}
	return f.raw, nil
}
func (f *nftWireOwnerFixture) rules(context.Context, string, int) ([]syscall.NetlinkMessage, error) {
	f.trace = append(f.trace, "rules")
	if f.fail == "rules" || f.fail == "rules-and-close" {
		return nil, f.failure
	}
	return f.rows, nil
}
func (f *nftWireOwnerFixture) close() error {
	f.trace = append(f.trace, "close")
	f.closes++
	if f.fail == "close" || f.fail == "rules-and-close" {
		return f.closeFailure
	}
	return nil
}
func (f *nftWireOwnerFixture) delete(_ context.Context, raw []byte) error {
	f.trace = append(f.trace, "delete")
	f.deleted = append([]byte(nil), raw...)
	return nil
}
func TestPlannedNFTVerifyOwnerOrderAndFailureCleanup(t *testing.T) {
	t.Run("same-owner-closed-read-order", func(t *testing.T) {
		p := nftWireFixturePlan(t)
		f := nftWireFixtureOwner(t, p)
		n := linuxPlannedNFT{blocked: &plannedNFTBlockedEvidence{}}
		if err := n.verifyOwnedRead(context.Background(), p, 1, true, f); err != nil {
			t.Fatal("real verification core rejected exact fixture", err)
		}
		want := []string{"open", "generation", "tables", "json", "rules", "generation", "close"}
		if !reflect.DeepEqual(f.trace, want) || f.closes != 1 || len(f.deleted) != 0 {
			t.Fatal("read bracket or close ownership changed", f.trace)
		}
	})
	for _, stage := range []string{"open", "before", "tables", "json", "rules", "after", "close"} {
		t.Run(stage, func(t *testing.T) {
			p := nftWireFixturePlan(t)
			f := nftWireFixtureOwner(t, p)
			f.fail = stage
			n := linuxPlannedNFT{blocked: &plannedNFTBlockedEvidence{}}
			err := n.verifyOwnedRead(context.Background(), p, 1, true, f)
			wanted := f.failure
			if stage == "close" {
				wanted = f.closeFailure
			}
			if !errors.Is(err, wanted) {
				t.Fatal("verification lost original stage/close failure", err)
			}
			closes := 1
			if stage == "open" {
				closes = 0
			}
			if f.closes != closes || len(f.deleted) != 0 {
				t.Fatal("failed read leaked or deleted", f.trace)
			}
		})
	}
	t.Run("read-and-close-errors-join", func(t *testing.T) {
		p := nftWireFixturePlan(t)
		f := nftWireFixtureOwner(t, p)
		f.fail = "rules-and-close"
		f.rows = nil
		n := linuxPlannedNFT{blocked: &plannedNFTBlockedEvidence{}}
		err := n.verifyOwnedRead(context.Background(), p, 1, true, f)
		if !errors.Is(err, f.failure) || !errors.Is(err, f.closeFailure) || f.closes != 1 {
			t.Fatal("read refusal or close failure was lost", err, f.trace)
		}
	})
	t.Run("cancelled-before-open-no-effects", func(t *testing.T) {
		p := nftWireFixturePlan(t)
		f := nftWireFixtureOwner(t, p)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		n := linuxPlannedNFT{}
		if err := n.verifyOwnedRead(ctx, p, 1, true, f); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(f.trace, []string{"open"}) {
			t.Fatal("cancelled verification proceeded", err, f.trace)
		}
	})
}
func TestPlannedNFTRemoveRequiresPositiveExactWitness(t *testing.T) {
	t.Run("exact-read-close-before-delete", func(t *testing.T) {
		p := nftWireFixturePlan(t)
		f := nftWireFixtureOwner(t, p)
		n := linuxPlannedNFT{blocked: &plannedNFTBlockedEvidence{defective: true}}
		if err := n.removeRead(context.Background(), p, 1, f); err != nil {
			t.Fatal("owned cleanup refused original exact policy", err)
		}
		want := []string{"tables", "open", "generation", "tables", "json", "rules", "generation", "close", "delete"}
		if !reflect.DeepEqual(f.trace, want) || f.closes != 1 {
			t.Fatal("cleanup lost complete guard read/close before effect", f.trace)
		}
		expected, _ := json.Marshal(map[string]any{"nftables": []any{map[string]any{"delete": map[string]any{"table": map[string]any{"family": "bridge", "name": p.bridge, "handle": uint64(1)}}}}})
		if string(f.deleted) != string(expected) || !n.blocked.defective {
			t.Fatal("cleanup changed exact deletion or erased defect")
		}
	})
	for _, name := range []string{"changed-generation", "meta-protocol-only", "close-failure", "foreign-handle", "missing-table"} {
		t.Run(name, func(t *testing.T) {
			p := nftWireFixturePlan(t)
			f := nftWireFixtureOwner(t, p)
			n := linuxPlannedNFT{blocked: &plannedNFTBlockedEvidence{}}
			switch name {
			case "changed-generation":
				f.after = 43
			case "meta-protocol-only":
				f.rows[0] = nftWireFixtureRule(p.bridge, 5, 0, 42, append(append(nftWireFixtureMeta(1, 1), nftWireFixtureCmp(1, 0, []byte{8, 0})...), nftWireFixtureTail(1)...))
			case "close-failure":
				f.fail = "close"
			case "foreign-handle":
				f.handle = 2
			case "missing-table":
				f.exists = false
			}
			err := n.removeRead(context.Background(), p, 1, f)
			if name == "missing-table" {
				if err != nil || !reflect.DeepEqual(f.trace, []string{"tables"}) {
					t.Fatal("absent table acquired cleanup effects", err, f.trace)
				}
			} else if err == nil {
				t.Fatal("unvalidated cleanup admitted", name)
			}
			if len(f.deleted) != 0 {
				t.Fatal("failed witness authorized delete", name, f.trace)
			}
		})
	}
}
