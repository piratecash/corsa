//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strconv"
	"time"
)

type plannedNFTBlockedEvidence struct {
	seen           bool
	packets, bytes uint64
	defective      bool
}
type linuxPlannedNFT struct {
	binary  string
	sha     ExecutableSHA256
	blocked *plannedNFTBlockedEvidence
}
type plannedNFTOutput struct {
	data     []byte
	overflow bool
}

func (w *plannedNFTOutput) Write(p []byte) (int, error) {
	if len(p) > processFileLimit-len(w.data) {
		w.overflow = true
		return 0, errPlannedNetwork
	}
	w.data = append(w.data, p...)
	return len(p), nil
}
func (n linuxPlannedNFT) run(ctx context.Context, input []byte, args ...string) ([]byte, error) {
	raw, _, err := n.runTracked(ctx, input, args...)
	return raw, err
}
func (n linuxPlannedNFT) runTracked(ctx context.Context, input []byte, args ...string) ([]byte, bool, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, false, ErrLinuxOwnerBound
	}
	if err := auditLinuxExecutable(ctx, n.binary, n.sha); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	command := exec.CommandContext(ctx, n.binary, args...)
	command.Env = []string{"PATH=/usr/bin:/bin", "LC_ALL=C"}
	command.Stdin = bytes.NewReader(input)
	output := &plannedNFTOutput{}
	diagnostic := &plannedNFTOutput{}
	command.Stdout = output
	command.Stderr = diagnostic
	// Reuse the caller's existing bound; no new runtime/profile numeric limit.
	command.WaitDelay = time.Until(deadline)
	if err := command.Start(); err != nil {
		return nil, false, plannedNetworkPhaseError(fmt.Sprintf("nft-command-start args=%q", args), errors.Join(errPlannedNetwork, err))
	}
	if err := command.Wait(); err != nil {
		return nil, true, plannedNetworkPhaseError(fmt.Sprintf("nft-command-wait args=%q stdout_bytes=%d stderr_bytes=%d stderr_prefix=%q", args, len(output.data), len(diagnostic.data), diagnostic.data[:min(len(diagnostic.data), 2048)]), errors.Join(errPlannedNetwork, err))
	}
	if output.overflow || diagnostic.overflow || len(diagnostic.data) != 0 {
		return nil, true, plannedNetworkPhaseError(fmt.Sprintf("nft-command-output args=%q stdout_bytes=%d stderr_bytes=%d stdout_overflow=%t stderr_overflow=%t stderr_prefix=%q", args, len(output.data), len(diagnostic.data), output.overflow, diagnostic.overflow, diagnostic.data[:min(len(diagnostic.data), 2048)]), errPlannedNetwork)
	}
	return output.data, true, nil
}
func plannedNFTMatch(left any, right any) map[string]any {
	return map[string]any{"match": map[string]any{"op": "==", "left": left, "right": right}}
}
func plannedNFTPayload(protocol, field string) map[string]any {
	return map[string]any{"payload": map[string]any{"protocol": protocol, "field": field}}
}
func plannedNFTObjects(p plannedNetworkPlan) []map[string]any {
	result := []map[string]any{{"table": map[string]any{"family": "bridge", "name": p.bridge, "comment": plannedNetworkAlias(p.nonce)}}, {"chain": map[string]any{"family": "bridge", "table": p.bridge, "name": "forward", "type": "filter", "hook": "forward", "prio": 0, "policy": "drop"}}}
	if len(p.members) != 0 {
		// Tuple membership binds each interface to its own exact address. Two
		// independent interface/address sets would permit source/destination spoofing.
		concat := func(values ...any) map[string]any { return map[string]any{"concat": values} }
		lookup := func(left any, values []any, op string) map[string]any {
			return map[string]any{"match": map[string]any{"op": op, "left": left, "right": map[string]any{"set": values}}}
		}
		members := make([]any, 0, len(p.members))
		ports := make([]any, 0, len(p.members))
		addresses := make([]any, 0, len(p.members))
		diagonal := make([]any, 0, len(p.members))
		for _, member := range p.members {
			members = append(members, concat(member.hostLink, member.address.String()))
			ports = append(ports, member.hostLink)
			addresses = append(addresses, member.address.String())
			diagonal = append(diagonal, concat(member.hostLink, member.hostLink))
		}
		input := map[string]any{"meta": map[string]any{"key": "iifname"}}
		output := map[string]any{"meta": map[string]any{"key": "oifname"}}
		// Invert a constant diagonal tuple-set lookup, not a dynamic comparison
		// between two registers. This excludes precisely the same original port.
		different := lookup(concat(input, output), diagonal, "!=")
		ip := []any{
			plannedNFTMatch(plannedNFTPayload("ether", "type"), "ip"),
			lookup(concat(input, plannedNFTPayload("ip", "saddr")), members, "=="),
			lookup(concat(output, plannedNFTPayload("ip", "daddr")), members, "=="),
			different, map[string]any{"counter": nil}, map[string]any{"accept": nil},
		}
		arp := []any{
			plannedNFTMatch(plannedNFTPayload("ether", "type"), "arp"),
			lookup(concat(input, plannedNFTPayload("arp", "saddr ip")), members, "=="),
			lookup(output, ports, "=="),
			lookup(plannedNFTPayload("arp", "daddr ip"), addresses, "=="),
			different, map[string]any{"counter": nil}, map[string]any{"accept": nil},
		}
		for _, expr := range [][]any{ip, arp} {
			result = append(result, map[string]any{"rule": map[string]any{"family": "bridge", "table": p.bridge, "chain": "forward", "expr": expr}})
		}
	}
	// A final explicit counter makes blocked traffic observable. Chain policy
	// is also drop; no accept rule permits IPv6, unplannedpeer or foreignsource.
	result = append(result, map[string]any{"rule": map[string]any{"family": "bridge", "table": p.bridge, "chain": "forward", "expr": []any{map[string]any{"counter": nil}, map[string]any{"drop": nil}}}})
	return result
}
func plannedNFTJSON(raw []byte) ([]map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var body struct {
		NFTables []map[string]any `json:"nftables"`
	}
	if err := decoder.Decode(&body); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errPlannedNetwork
	}
	if body.NFTables == nil {
		return nil, errPlannedNetwork
	}
	return body.NFTables, nil
}
func (n linuxPlannedNFT) tables(ctx context.Context, name string) (uint64, bool, error) {
	raw, err := n.run(ctx, nil, "--json", "--handle", "list", "tables")
	if err != nil {
		return 0, false, err
	}
	objects, err := plannedNFTJSON(raw)
	if err != nil {
		return 0, false, err
	}
	var found uint64
	for _, object := range objects {
		if len(object) != 1 {
			return 0, false, errPlannedNetwork
		}
		value, ok := object["table"]
		if !ok {
			if _, meta := object["metainfo"]; meta {
				continue
			}
			return 0, false, errPlannedNetwork
		}
		table, ok := value.(map[string]any)
		if !ok {
			return 0, false, errPlannedNetwork
		}
		if table["family"] != "bridge" || table["name"] != name {
			continue
		}
		handle, ok := table["handle"].(json.Number)
		if !ok || found != 0 {
			return 0, false, errPlannedNetwork
		}
		found, err = strconv.ParseUint(string(handle), 10, 64)
		if err != nil || found == 0 {
			return 0, false, errPlannedNetwork
		}
	}
	return found, found != 0, nil
}
func (n linuxPlannedNFT) create(ctx context.Context, p plannedNetworkPlan) (uint64, bool, error) {
	if handle, exists, err := n.tables(ctx, p.bridge); err != nil || exists {
		return 0, false, plannedNetworkPhaseError(fmt.Sprintf("nft-create-preflight table=%s handle=%d exists=%t", p.bridge, handle, exists), errors.Join(errPlannedNetwork, err))
	}
	objects := plannedNFTObjects(p)
	commands := make([]map[string]any, 0, len(objects))
	for _, object := range objects {
		commands = append(commands, map[string]any{"add": object})
	}
	raw, err := json.Marshal(map[string]any{"nftables": commands})
	if err != nil {
		return 0, false, plannedNetworkPhaseError("nft-create-marshal", err)
	}
	_, attempted, createErr := n.runTracked(ctx, raw, "--json", "--file", "-")
	if !attempted {
		return 0, false, plannedNetworkPhaseError("nft-create-not-started", createErr)
	}
	handle, exists, lookupErr := n.tables(ctx, p.bridge)
	if createErr != nil || lookupErr != nil || !exists {
		return handle, true, plannedNetworkPhaseError(fmt.Sprintf("nft-create-readback table=%s handle=%d exists=%t", p.bridge, handle, exists), errors.Join(errPlannedNetwork, createErr, lookupErr))
	}
	return handle, true, plannedNetworkPhaseError("nft-create-verify", n.verify(ctx, p, handle))
}
func normalizePlannedNFT(value any) (any, error) {
	switch v := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for key, child := range v {
			if key == "handle" || key == "index" {
				number, ok := child.(json.Number)
				if !ok {
					return nil, errPlannedNetwork
				}
				if _, err := strconv.ParseUint(string(number), 10, 64); err != nil {
					return nil, errPlannedNetwork
				}
				continue
			}
			if key == "counter" && child != nil {
				counter, ok := child.(map[string]any)
				if !ok || len(counter) != 2 {
					return nil, errPlannedNetwork
				}
				for _, field := range []string{"packets", "bytes"} {
					number, ok := counter[field].(json.Number)
					if !ok {
						return nil, errPlannedNetwork
					}
					if _, err := strconv.ParseUint(string(number), 10, 64); err != nil {
						return nil, errPlannedNetwork
					}
				}
				out[key] = nil
				continue
			}
			normalized, err := normalizePlannedNFT(child)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case []map[string]any:
		out := make([]any, len(v))
		for i, child := range v {
			var err error
			out[i], err = normalizePlannedNFT(child)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			var err error
			out[i], err = normalizePlannedNFT(child)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case json.Number:
		return string(v), nil
	case int:
		return strconv.Itoa(v), nil
	default:
		return value, nil
	}
}
func plannedNFTBlockedCounters(objects []map[string]any) (uint64, uint64, bool) {
	if len(objects) == 0 {
		return 0, 0, false
	}
	rule, ok := objects[len(objects)-1]["rule"].(map[string]any)
	if !ok || rule["chain"] != "forward" {
		return 0, 0, false
	}
	expressions, ok := rule["expr"].([]any)
	if !ok || len(expressions) != 2 {
		return 0, 0, false
	}
	first, ok := expressions[0].(map[string]any)
	if !ok || len(first) != 1 {
		return 0, 0, false
	}
	counter, ok := first["counter"].(map[string]any)
	if !ok || len(counter) != 2 {
		return 0, 0, false
	}
	values := make([]uint64, 2)
	for i, field := range []string{"packets", "bytes"} {
		number, ok := counter[field].(json.Number)
		if !ok {
			return 0, 0, false
		}
		value, err := strconv.ParseUint(string(number), 10, 64)
		if err != nil {
			return 0, 0, false
		}
		values[i] = value
	}
	last, ok := expressions[1].(map[string]any)
	if !ok || len(last) != 1 {
		return 0, 0, false
	}
	drop, ok := last["drop"]
	return values[0], values[1], ok && drop == nil
}
func plannedNFTBlockedCountersClear(objects []map[string]any) bool {
	packets, bytes, ok := plannedNFTBlockedCounters(objects)
	return ok && packets == 0 && bytes == 0
}
func (n linuxPlannedNFT) retainBlockedEvidence(objects []map[string]any) bool {
	packets, bytes, ok := plannedNFTBlockedCounters(objects)
	if !ok {
		return false
	}
	if n.blocked != nil {
		n.blocked.seen = true
		n.blocked.packets = max(n.blocked.packets, packets)
		n.blocked.bytes = max(n.blocked.bytes, bytes)
		n.blocked.defective = n.blocked.defective || packets != 0 || bytes != 0
	}
	return true
}
func (n linuxPlannedNFT) operationallyClear(objects []map[string]any) bool {
	return plannedNFTBlockedCountersClear(objects) && (n.blocked == nil || !n.blocked.defective)
}
func (n linuxPlannedNFT) verify(ctx context.Context, p plannedNetworkPlan, expected uint64) error {
	return n.verifyOwned(ctx, p, expected, true)
}
func (n linuxPlannedNFT) verifyOwned(ctx context.Context, p plannedNetworkPlan, expected uint64, requireClear bool) error {
	// Keep the CLI child and read-only witness in the same original namespace.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return n.verifyOwnedRead(ctx, p, expected, requireClear, plannedNFTRealRead{nft: n})
}
func (n linuxPlannedNFT) verifyOwnedRead(ctx context.Context, p plannedNetworkPlan, expected uint64, requireClear bool, ops plannedNFTReadbackOps) (resultErr error) {
	wire, err := ops.openWire(ctx)
	if err != nil {
		return plannedNetworkPhaseError("nft-wire-open", err)
	}
	defer func() { resultErr = errors.Join(resultErr, wire.close()) }()
	before, err := wire.generation(ctx)
	if err != nil {
		return plannedNetworkPhaseError("nft-wire-generation-before", err)
	}
	actual, exists, err := ops.tables(ctx, p.bridge)
	if err != nil || !exists || expected == 0 || actual != expected {
		return plannedNetworkPhaseError(fmt.Sprintf("nft-verify-table table=%s actual_handle=%d expected_handle=%d exists=%t", p.bridge, actual, expected, exists), errors.Join(errPlannedNetwork, err))
	}
	raw, err := ops.list(ctx, p.bridge)
	if err != nil {
		return plannedNetworkPhaseError("nft-verify-list-table", err)
	}
	objects, err := plannedNFTJSON(raw)
	if err != nil {
		return plannedNetworkPhaseError(fmt.Sprintf("nft-verify-json raw_bytes=%d raw_prefix=%q", len(raw), raw[:min(len(raw), 4096)]), err)
	}
	filtered := []map[string]any{}
	for _, object := range objects {
		if _, meta := object["metainfo"]; meta {
			if len(object) != 1 {
				return plannedNetworkPhaseError("nft-verify-metainfo-object", errPlannedNetwork)
			}
			continue
		}
		filtered = append(filtered, object)
	}
	if !n.retainBlockedEvidence(filtered) {
		return plannedNetworkPhaseError(fmt.Sprintf("nft-verify-blocked-counter-shape raw_bytes=%d raw_prefix=%q", len(raw), raw[:min(len(raw), 4096)]), errPlannedNetwork)
	}
	if requireClear && !n.operationallyClear(filtered) {
		return plannedNetworkPhaseError(fmt.Sprintf("nft-verify-blocked-counter-not-clear evidence=%+v raw_bytes=%d raw_prefix=%q", n.blocked, len(raw), raw[:min(len(raw), 4096)]), errPlannedNetwork)
	}
	// One unchanged generation brackets the original table/JSON reads and
	// the raw witness. A changed/incomplete cut refuses without retry.
	count := 1
	if len(p.members) != 0 {
		count = 3
	}
	rows, err := wire.rules(ctx, p.bridge, count)
	if err != nil {
		return plannedNetworkPhaseError("nft-wire-rules", err)
	}
	after, err := wire.generation(ctx)
	if err != nil {
		return plannedNetworkPhaseError("nft-wire-generation-after", err)
	}
	if err = plannedNFTExactReadback(p, filtered, rows, before, after); err != nil {
		return plannedNetworkPhaseError(fmt.Sprintf("nft-verify-exact-object-or-wire-mismatch raw_bytes=%d raw_prefix=%q", len(raw), raw[:min(len(raw), 4096)]), err)
	}

	return nil
}
func (n linuxPlannedNFT) remove(ctx context.Context, p plannedNetworkPlan, expected uint64) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return n.removeRead(ctx, p, expected, plannedNFTRealRead{nft: n})
}
func (n linuxPlannedNFT) removeRead(ctx context.Context, p plannedNetworkPlan, expected uint64, ops plannedNFTReadbackOps) error {
	actual, exists, err := ops.tables(ctx, p.bridge)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	// An uncertain create can bind only this exact original nonce-tagged full
	// table; it cannot turn a same-name replacement into cleanup authority.
	if expected != 0 && actual != expected {
		return errPlannedNetwork
	}
	if err = n.verifyOwnedRead(ctx, p, actual, false, ops); err != nil {
		return err
	}
	raw, err := json.Marshal(map[string]any{"nftables": []any{map[string]any{"delete": map[string]any{"table": map[string]any{"family": "bridge", "name": p.bridge, "handle": actual}}}}})
	if err != nil {
		return err
	}
	return ops.delete(ctx, raw)
}
