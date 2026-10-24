//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// These private ABI values are from include/linux/netfilter/nf_tables.h in
// official libnftnl 1.3.1. No netlink write operation is supported here.
const (
	plannedNFTNewRule = 10<<8 | 6
	plannedNFTGetRule = 10<<8 | 7
	plannedNFTNewGen  = 10<<8 | 15
	plannedNFTGetGen  = 10<<8 | 16
)

// A closed per-call dependency seam for the real owner and deterministic
// tests. No mutable field/global hook or runtime installer can select it.
type plannedNFTReadbackWire interface {
	generation(context.Context) (uint32, error)
	rules(context.Context, string, int) ([]syscall.NetlinkMessage, error)
	close() error
}
type plannedNFTReadbackOps interface {
	openWire(context.Context) (plannedNFTReadbackWire, error)
	tables(context.Context, string) (uint64, bool, error)
	list(context.Context, string) ([]byte, error)
	delete(context.Context, []byte) error
}
type plannedNFTRealRead struct{ nft linuxPlannedNFT }

func (o plannedNFTRealRead) openWire(ctx context.Context) (plannedNFTReadbackWire, error) {
	return openPlannedNFTWire(ctx)
}
func (o plannedNFTRealRead) tables(ctx context.Context, name string) (uint64, bool, error) {
	return o.nft.tables(ctx, name)
}
func (o plannedNFTRealRead) list(ctx context.Context, name string) ([]byte, error) {
	return o.nft.run(ctx, nil, "--json", "--handle", "list", "table", "bridge", name)
}
func (o plannedNFTRealRead) delete(ctx context.Context, raw []byte) error {
	_, err := o.nft.run(ctx, raw, "--json", "--file", "-")
	return err
}

type plannedNFTWire struct {
	fd, namespaceFD int
	port, seq       uint32
	namespace       linuxNamespaceIdentity
}

func openPlannedNFTWire(ctx context.Context) (_ *plannedNFTWire, resultErr error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrLinuxOwnerBound
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ns, identity, err := openThreadNetNS()
	if err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_NETFILTER)
	if err != nil {
		return nil, errors.Join(err, unix.Close(ns))
	}
	w := &plannedNFTWire{fd: fd, namespaceFD: ns, namespace: identity}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, w.close())
		}
	}()
	if err = unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return nil, err
	}
	name, err := unix.Getsockname(fd)
	if err != nil {
		return nil, err
	}
	local, ok := name.(*unix.SockaddrNetlink)
	if !ok || local.Family != unix.AF_NETLINK || local.Pid == 0 || local.Groups != 0 {
		return nil, errPlannedNetwork
	}
	w.port = local.Pid
	return w, nil
}
func (w *plannedNFTWire) close() error {
	var err error
	if w.fd >= 0 {
		fd := w.fd
		w.fd = -1
		err = unix.Close(fd)
	}
	if w.namespaceFD >= 0 {
		fd := w.namespaceFD
		w.namespaceFD = -1
		err = errors.Join(err, unix.Close(fd))
	}
	return err
}
func (w *plannedNFTWire) current() error {
	fd, identity, err := openThreadNetNS()
	if err != nil {
		return err
	}
	closeErr := unix.Close(fd)
	if identity != w.namespace {
		return errors.Join(errPlannedNetwork, closeErr)
	}
	return closeErr
}

// Same caller-deadline, kernel-sender and sequence mechanics as the original
// plannedRouteSocket, with closed read-only requests, Recvmsg truncation flags,
// total byte/count limits and exact response completion. Route behavior is not changed.
func (w *plannedNFTWire) read(ctx context.Context, kind uint16, table string, limit int) ([]syscall.NetlinkMessage, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, ErrLinuxOwnerBound
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.current(); err != nil {
		return nil, err
	}
	dump := kind == plannedNFTGetRule
	if w.fd < 0 || w.port == 0 || w.seq == math.MaxUint32 || limit < 1 || limit > 3 || (!dump && (kind != plannedNFTGetGen || table != "" || limit != 1)) || (dump && table == "") {
		return nil, errPlannedNetwork
	}
	w.seq++
	body := []byte{0, 0, 0, 0}
	flags := uint16(unix.NLM_F_REQUEST)
	expected := uint16(plannedNFTNewGen)
	if dump {
		body[0] = unix.AF_BRIDGE
		body = append(body, plannedStringAttribute(1, table)...)
		body = append(body, plannedStringAttribute(2, "forward")...)
		flags = unix.NLM_F_REQUEST | unix.NLM_F_DUMP
		expected = plannedNFTNewRule
	}
	request := make([]byte, unix.NLMSG_HDRLEN+len(body))
	binary.NativeEndian.PutUint32(request, uint32(len(request)))
	binary.NativeEndian.PutUint16(request[4:], kind)
	binary.NativeEndian.PutUint16(request[6:], flags)
	binary.NativeEndian.PutUint32(request[8:], w.seq)
	binary.NativeEndian.PutUint32(request[12:], w.port)
	copy(request[unix.NLMSG_HDRLEN:], body)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, context.DeadlineExceeded
	}
	tv := unix.NsecToTimeval(remaining.Nanoseconds())
	if err := unix.SetsockoptTimeval(w.fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, &tv); err != nil {
		return nil, err
	}
	if err := unix.Sendto(w.fd, request, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return nil, err
	}
	replies := plannedNFTReplies{seq: w.seq, port: w.port, kind: expected, dump: dump, limit: limit, request: request}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining = time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		tv = unix.NsecToTimeval(remaining.Nanoseconds())
		if err := unix.SetsockoptTimeval(w.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
			return nil, err
		}
		buffer := make([]byte, processFileLimit)
		n, _, recvFlags, from, err := unix.Recvmsg(w.fd, buffer, nil, 0)
		if err != nil {
			return nil, err
		}
		if n < 0 || n >= len(buffer) {
			return nil, errPlannedNetwork
		}
		if err := replies.datagram(buffer[:n], from, recvFlags); err != nil {
			return nil, err
		}
		// Inspect every message in a datagram before accepting a terminal marker.
		if replies.complete() {
			if err := w.current(); err != nil {
				return nil, err
			}
			return replies.rows, nil
		}
	}
}

type plannedNFTReplies struct {
	seq, port uint32
	kind      uint16
	dump      bool
	limit     int
	request   []byte
	rows      []syscall.NetlinkMessage
	done      bool
	bytes     int
}

func (r *plannedNFTReplies) datagram(raw []byte, from unix.Sockaddr, recvFlags int) error {
	peer, ok := from.(*unix.SockaddrNetlink)
	if !ok || peer.Family != unix.AF_NETLINK || peer.Pid != 0 || peer.Groups != 0 || recvFlags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || len(raw) == 0 || len(raw) >= processFileLimit || len(raw) > processFileLimit-r.bytes {
		return errPlannedNetwork
	}
	r.bytes += len(raw)
	messages, err := syscall.ParseNetlinkMessage(raw)
	if err != nil || len(messages) == 0 {
		return errors.Join(errPlannedNetwork, err)
	}
	framed := 0
	for _, message := range messages {
		framed += plannedAlign(int(message.Header.Len))
		if err := r.accept(message); err != nil {
			return err
		}
	}
	if framed != len(raw) {
		return errPlannedNetwork
	}
	return nil
}
func (r *plannedNFTReplies) accept(m syscall.NetlinkMessage) error {
	if m.Header.Seq != r.seq || m.Header.Pid != r.port || m.Header.Flags&unix.NLM_F_DUMP_INTR != 0 || r.done {
		return errPlannedNetwork
	}
	switch m.Header.Type {
	case unix.NLMSG_ERROR:
		// No success ACK is requested; only a typed NEWGEN or completed rule dump
		// can succeed. Preserve a kernel errno, but never treat ERROR as evidence.
		if len(m.Data) < 4+unix.NLMSG_HDRLEN || !bytes.Equal(m.Data[4:4+unix.NLMSG_HDRLEN], r.request[:unix.NLMSG_HDRLEN]) {
			return errPlannedNetwork
		}
		code := int32(binary.NativeEndian.Uint32(m.Data))
		if code < 0 {
			return syscall.Errno(-code)
		}
		return errPlannedNetwork
	case unix.NLMSG_DONE:
		if !r.dump || m.Header.Flags != unix.NLM_F_MULTI || len(m.Data) != 4 {
			return errPlannedNetwork
		}
		code := int32(binary.NativeEndian.Uint32(m.Data))
		if code < 0 {
			return syscall.Errno(-code)
		}
		if code > 0 {
			return errPlannedNetwork
		}
		r.done = true
	default:
		flags := uint16(0)
		if r.dump {
			flags = unix.NLM_F_MULTI | unix.NLM_F_APPEND
		}
		if m.Header.Type != r.kind || m.Header.Flags != flags || len(r.rows) >= r.limit {
			return errPlannedNetwork
		}
		r.rows = append(r.rows, m)
	}
	return nil
}
func (r *plannedNFTReplies) complete() bool {
	if r.dump {
		return r.done
	}
	return len(r.rows) == 1
}

// Kernel nft dumps use legacy no-flag nests. Accept the explicit NESTED bit
// only at declared nested attributes; reject all other flag/attribute variants.
func plannedNFTAttributes(raw []byte, nested map[uint16]bool, pad uint16) (map[uint16][]byte, error) {
	out := make(map[uint16][]byte)
	for len(raw) > 0 {
		if len(raw) < 4 {
			return nil, errPlannedNetwork
		}
		n := int(binary.NativeEndian.Uint16(raw))
		kind := binary.NativeEndian.Uint16(raw[2:])
		base := kind & 0x3fff
		if n < 4 || plannedAlign(n) > len(raw) {
			return nil, errPlannedNetwork
		}
		allow, isKnown := nested[base]
		if !isKnown || kind != base && (!allow || kind != base|unix.NLA_F_NESTED) {
			return nil, errPlannedNetwork
		}
		for _, v := range raw[n:plannedAlign(n)] {
			if v != 0 {
				return nil, errPlannedNetwork
			}
		}
		if base == pad && pad != 0 {
			if n != 4 {
				return nil, errPlannedNetwork
			}
		} else {
			if _, exists := out[base]; exists {
				return nil, errPlannedNetwork
			}
			out[base] = raw[4:n]
		}
		raw = raw[plannedAlign(n):]
	}
	return out, nil
}
func plannedNFTU32(raw []byte) (uint32, bool) {
	if len(raw) != 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(raw), true
}
func plannedNFTString(raw []byte) (string, bool) {
	if len(raw) < 2 || raw[len(raw)-1] != 0 || bytes.IndexByte(raw[:len(raw)-1], 0) >= 0 {
		return "", false
	}
	return string(raw[:len(raw)-1]), true
}
func plannedNFTGeneration(m syscall.NetlinkMessage) (uint32, error) {
	if m.Header.Type != plannedNFTNewGen || len(m.Data) < 4 || m.Data[0] != unix.AF_UNSPEC || m.Data[1] != 0 {
		return 0, errPlannedNetwork
	}
	a, err := plannedNFTAttributes(m.Data[4:], map[uint16]bool{1: false, 2: false, 3: false}, 0)
	if err != nil || len(a) != 3 {
		return 0, errors.Join(errPlannedNetwork, err)
	}
	id, ok := plannedNFTU32(a[1])
	pid, pidOK := plannedNFTU32(a[2])
	name, nameOK := plannedNFTString(a[3])
	if !ok || id == 0 || !pidOK || pid == 0 || !nameOK || len(name) > 15 || binary.BigEndian.Uint16(m.Data[2:]) != uint16(id) {
		return 0, errPlannedNetwork
	}
	return id, nil
}
func (w *plannedNFTWire) rules(ctx context.Context, table string, count int) ([]syscall.NetlinkMessage, error) {
	return w.read(ctx, plannedNFTGetRule, table, count)
}
func (w *plannedNFTWire) generation(ctx context.Context) (uint32, error) {
	rows, err := w.read(ctx, plannedNFTGetGen, "", 1)
	if err != nil {
		return 0, err
	}
	return plannedNFTGeneration(rows[0])
}

type plannedNFTWireExpression struct {
	name  string
	attrs map[uint16][]byte
}
type plannedNFTWireRule struct {
	handle, previous uint64
	expressions      []plannedNFTWireExpression
}

func plannedNFTWireExpressions(raw []byte) ([]plannedNFTWireExpression, error) {
	var out []plannedNFTWireExpression
	for len(raw) > 0 {
		if len(out) >= 64 || len(raw) < 4 {
			return nil, errPlannedNetwork
		}
		n := int(binary.NativeEndian.Uint16(raw))
		kind := binary.NativeEndian.Uint16(raw[2:])
		if n < 4 || plannedAlign(n) > len(raw) || (kind != 1 && kind != 1|unix.NLA_F_NESTED) {
			return nil, errPlannedNetwork
		}
		e, err := plannedNFTAttributes(raw[4:n], map[uint16]bool{1: false, 2: true}, 0)
		if err != nil || len(e) != 2 {
			return nil, errors.Join(errPlannedNetwork, err)
		}
		name, ok := plannedNFTString(e[1])
		if !ok {
			return nil, errPlannedNetwork
		}
		var schema map[uint16]bool
		var pad uint16
		switch name {
		case "payload":
			schema = map[uint16]bool{1: false, 2: false, 3: false, 4: false}
		case "cmp":
			schema = map[uint16]bool{1: false, 2: false, 3: true}
		case "meta":
			schema = map[uint16]bool{1: false, 2: false}
		case "lookup":
			schema = map[uint16]bool{1: false, 2: false, 3: false, 4: false, 5: false}
		case "counter":
			schema = map[uint16]bool{1: false, 2: false, 3: false}
			pad = 3
		case "immediate":
			schema = map[uint16]bool{1: false, 2: true}
		default:
			return nil, errPlannedNetwork
		}
		attrs, err := plannedNFTAttributes(e[2], schema, pad)
		if err != nil {
			return nil, err
		}
		// Close the complete supported expression shape, not only the guard prefix.
		for id, value := range attrs {
			switch {
			case (name == "cmp" && id == 3) || (name == "immediate" && id == 2):
				data, err := plannedNFTAttributes(value, map[uint16]bool{1: false, 2: true}, 0)
				if err != nil || len(data) != 1 {
					return nil, errPlannedNetwork
				}
				if v, ok := data[1]; ok {
					if name != "cmp" || len(v) == 0 || len(v) > 64 {
						return nil, errPlannedNetwork
					}
				} else {
					verdict, err := plannedNFTAttributes(data[2], map[uint16]bool{1: false}, 0)
					if name != "immediate" || err != nil || len(verdict) != 1 || len(verdict[1]) != 4 {
						return nil, errPlannedNetwork
					}
				}
			case name == "lookup" && id == 1:
				if _, ok := plannedNFTString(value); !ok {
					return nil, errPlannedNetwork
				}
			case name == "counter":
				if len(value) != 8 {
					return nil, errPlannedNetwork
				}
			default:
				if len(value) != 4 {
					return nil, errPlannedNetwork
				}
			}
		}
		required := 0
		switch name {
		case "payload":
			required = 4
		case "cmp":
			required = 3
		case "meta", "counter", "immediate":
			required = 2
		case "lookup":
			if attrs[1] == nil || attrs[2] == nil {
				return nil, errPlannedNetwork
			}
		}
		if required != 0 && len(attrs) != required {
			return nil, errPlannedNetwork
		}
		out = append(out, plannedNFTWireExpression{name: name, attrs: attrs})
		for _, v := range raw[n:plannedAlign(n)] {
			if v != 0 {
				return nil, errPlannedNetwork
			}
		}
		raw = raw[plannedAlign(n):]
	}
	if len(out) < 2 {
		return nil, errPlannedNetwork
	}
	return out, nil
}
func plannedNFTDecodeRule(m syscall.NetlinkMessage, table string, generation uint32) (plannedNFTWireRule, error) {
	var out plannedNFTWireRule
	if m.Header.Type != plannedNFTNewRule || len(m.Data) < 4 || m.Data[0] != unix.AF_BRIDGE || m.Data[1] != 0 || binary.BigEndian.Uint16(m.Data[2:]) != uint16(generation) {
		return out, errPlannedNetwork
	}
	a, err := plannedNFTAttributes(m.Data[4:], map[uint16]bool{1: false, 2: false, 3: false, 4: true, 6: false, 8: false}, 8)
	if err != nil || len(a) < 4 || len(a) > 5 {
		return out, errors.Join(errPlannedNetwork, err)
	}
	name, nameOK := plannedNFTString(a[1])
	chain, chainOK := plannedNFTString(a[2])
	if !nameOK || name != table || !chainOK || chain != "forward" || len(a[3]) != 8 || a[4] == nil {
		return out, errPlannedNetwork
	}
	out.handle = binary.BigEndian.Uint64(a[3])
	if out.handle == 0 {
		return out, errPlannedNetwork
	}
	if previous, ok := a[6]; ok {
		if len(previous) != 8 {
			return out, errPlannedNetwork
		}
		out.previous = binary.BigEndian.Uint64(previous)
		if out.previous == 0 {
			return out, errPlannedNetwork
		}
	}
	out.expressions, err = plannedNFTWireExpressions(a[4])
	return out, err
}
func plannedNFTEtherGuard(expressions []plannedNFTWireExpression, etherType byte) bool {
	if len(expressions) < 2 || expressions[0].name != "payload" || expressions[1].name != "cmp" {
		return false
	}
	p, c := expressions[0].attrs, expressions[1].attrs
	if len(p) != 4 || len(c) != 3 {
		return false
	}
	reg, regOK := plannedNFTU32(p[1])
	base, baseOK := plannedNFTU32(p[2])
	offset, offOK := plannedNFTU32(p[3])
	length, lenOK := plannedNFTU32(p[4])
	sreg, sregOK := plannedNFTU32(c[1])
	op, opOK := plannedNFTU32(c[2])
	if !regOK || (reg < 1 || reg > 4 && reg < 8 || reg > 23) || !baseOK || base != 0 || !offOK || offset != 12 || !lenOK || length != 2 || !sregOK || sreg != reg || !opOK || op != 0 {
		return false
	}
	data, err := plannedNFTAttributes(c[3], map[uint16]bool{1: false}, 0)
	return err == nil && len(data) == 1 && bytes.Equal(data[1], []byte{8, etherType})
}
func plannedNFTWireMatches(p plannedNetworkPlan, objects []map[string]any, rows []syscall.NetlinkMessage, before, after uint32) error {
	if before == 0 || before != after {
		return errPlannedNetwork
	}
	count := 1
	if len(p.members) != 0 {
		count = 3
	}
	if len(rows) != count {
		return errPlannedNetwork
	}
	var handles []uint64
	for _, object := range objects {
		if raw, exists := object["rule"]; exists {
			rule, ok := raw.(map[string]any)
			if !ok {
				return errPlannedNetwork
			}
			number, ok := rule["handle"].(json.Number)
			if !ok {
				return errPlannedNetwork
			}
			handle, err := strconv.ParseUint(string(number), 10, 64)
			if err != nil || handle == 0 {
				return errPlannedNetwork
			}
			for _, seen := range handles {
				if seen == handle {
					return errPlannedNetwork
				}
			}
			handles = append(handles, handle)
		}
	}
	if len(handles) != count {
		return errPlannedNetwork
	}
	for i, row := range rows {
		rule, err := plannedNFTDecodeRule(row, p.bridge, before)
		previous := uint64(0)
		if i > 0 {
			previous = handles[i-1]
		}
		if err != nil || rule.handle != handles[i] || rule.previous != previous {
			return errors.Join(errPlannedNetwork, err)
		}
		if count == 3 && i < 2 {
			etherType := byte(0)
			if i == 1 {
				etherType = 6
			}
			if !plannedNFTEtherGuard(rule.expressions, etherType) {
				return errPlannedNetwork
			}
		}
	}
	return nil
}

// This expected-only readback alternative is permitted ONLY after the exact
// binary EtherType witness. Never erase fields from the actual JSON. A bridge
// meta-protocol guard alone can print the same JSON but fails that witness.
func plannedNFTElidedExpected(p plannedNetworkPlan) ([]map[string]any, error) {
	objects := plannedNFTObjects(p)
	if len(p.members) != 0 {
		if len(objects) != 5 {
			return nil, errPlannedNetwork
		}
		for i, protocol := range []string{"ip", "arp"} {
			rule, ok := objects[i+2]["rule"].(map[string]any)
			if !ok {
				return nil, errPlannedNetwork
			}
			expressions, ok := rule["expr"].([]any)
			expectedLength := 6 + i
			if !ok || len(expressions) != expectedLength || !reflect.DeepEqual(expressions[0], plannedNFTMatch(plannedNFTPayload("ether", "type"), protocol)) {
				return nil, errPlannedNetwork
			}
			rule["expr"] = expressions[1:]
		}
	}
	return objects, nil
}

func plannedNFTExactReadback(p plannedNetworkPlan, objects []map[string]any, rows []syscall.NetlinkMessage, before, after uint32) error {
	if err := plannedNFTWireMatches(p, objects, rows, before, after); err != nil {
		return err
	}
	actual, err := normalizePlannedNFT(objects)
	if err != nil {
		return err
	}
	original, err := normalizePlannedNFT(plannedNFTObjects(p))
	if err != nil {
		return err
	}
	elidedSource, err := plannedNFTElidedExpected(p)
	if err != nil {
		return err
	}
	elided, err := normalizePlannedNFT(elidedSource)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, original) && !reflect.DeepEqual(actual, elided) {
		return errPlannedNetwork
	}
	return nil
}
