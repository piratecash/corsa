"""Bounded SG32 value and file checks. Import performs no work."""
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import struct

IMAGE = "sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e"
INPUT_NAMES = {"corsad", "c2driver", "sg32_common.py", "sg32_service.py", "clockprobe.py"}
MEMORY = 512000000


class Fault(Exception):
    pass


def need(value, reason):
    if not value:
        raise Fault(reason)


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate-json-key")
            result[key] = value
        return result
    def invalid(_):
        raise Fault("nonfinite-json")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def encode(value, max_bytes=65536):
    raw = json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf8") + b"\n"
    need(len(raw) <= max_bytes, "record-limit")
    return raw


def stamp(st):
    return (st.st_dev, st.st_ino, st.st_mode, st.st_uid, st.st_gid,
            st.st_nlink, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def read_owned(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_uid == 501 and before.st_nlink == 1
             and not before.st_mode & 0o022 and 0 < before.st_size <= limit, "input-custody")
        raw = bytearray()
        while len(raw) <= limit:
            part = os.read(fd, min(65536, limit + 1 - len(raw)))
            if not part:
                break
            raw.extend(part)
        need(len(raw) == before.st_size and stamp(before) == stamp(os.fstat(fd)) ==
             stamp(Path(path).lstat()), "input-changed")
        return bytes(raw)
    finally:
        os.close(fd)


def static_elf(raw):
    need(len(raw) >= 64 and raw[:7] == b"\x7fELF\x02\x01\x01", "ELF64LE-required")
    fields = struct.unpack_from("<HHIQQQIHHHHHH", raw, 16)
    need(fields[0] == 2 and fields[1] == 183 and fields[2] == 1 and fields[7] == 64 and
         fields[8] == 56 and 0 < fields[9] <= 128, "static-arm64-ELF-header")
    offset, count = fields[4], fields[9]
    need(64 <= offset <= 65536 and offset + count * 56 <= min(len(raw), 65536), "ELF-phdr-bound")
    for index in range(count):
        row = struct.unpack_from("<IIQQQQQQ", raw, offset + 56 * index)
        need(row[0] not in (2, 3) and row[2] <= len(raw) and row[5] <= len(raw) - row[2],
             "ELF-dynamic-or-segment")
    return {"class": 64, "endian": "little", "machine": "aarch64", "dynamic": False}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def hex_value(value, width):
    return type(value) is str and re.fullmatch("[0-9a-f]{" + str(width) + "}", value) is not None and value != "0" * width


NETWORK_FILES = ("fib_trie", "if_inet6", "route", "ipv6_route", "tcp", "tcp6", "udp", "udp6")


def network_values(raws):
    """Closed loopback-only proc snapshots; no claim of an atomic network instant."""
    need(type(raws) is dict and set(raws) == set(NETWORK_FILES), "network-files")
    for raw in raws.values():
        need(type(raw) is bytes and 0 <= len(raw) <= 65536 and (not raw or raw.endswith(b"\n")), "network-read-bound")
    leaves = 0
    for line in raws["fib_trie"].decode("ascii").splitlines():
        if "|--" in line:
            match = re.fullmatch(r"\s*\|-- ([0-9.]+)\s*", line)
            need(match is not None and ipaddress.IPv4Address(match.group(1)).is_loopback, "nonloopback-v4-address")
            leaves += 1
    need(leaves > 0 and "LOCAL" in raws["fib_trie"].decode("ascii"), "v4-address-proof-empty")
    for line in raws["if_inet6"].decode("ascii").splitlines():
        parts = line.split()
        need(len(parts) == 6 and re.fullmatch("[0-9a-fA-F]{32}", parts[0]) is not None and
             ipaddress.IPv6Address(bytes.fromhex(parts[0])).is_loopback and parts[5] == "lo", "nonloopback-v6-address")
    route_lines = raws["route"].decode("ascii").splitlines()
    need(bool(route_lines) and route_lines[0].split()[:4] == ["Iface", "Destination", "Gateway", "Flags"], "v4-route-header")
    for line in route_lines[1:]:
        p = line.split()
        need(len(p) == 11 and p[0] == "lo" and
             all(re.fullmatch("[0-9a-fA-F]{8}", p[i]) is not None for i in (1, 2, 7)) and
             re.fullmatch("[0-9a-fA-F]{1,8}", p[3]) is not None and int(p[2], 16) == 0,
             "v4-route-shape")
        dest = ipaddress.IPv4Address(bytes.fromhex(p[1])[::-1])
        mask = ipaddress.IPv4Address(bytes.fromhex(p[7])[::-1])
        route = ipaddress.IPv4Network(str(dest) + "/" + str(mask), strict=False)
        need(route.netmask == mask and route.prefixlen >= 8 and route.network_address.is_loopback, "nonloopback-v4-route")
    for line in raws["ipv6_route"].decode("ascii").splitlines():
        p = line.split()
        need(len(p) == 10 and p[9] == "lo" and all(re.fullmatch("[0-9a-fA-F]{32}", p[i]) is not None for i in (0, 2, 4)),
             "v6-route-shape")
        dest = ipaddress.IPv6Address(bytes.fromhex(p[0]))
        need(int(p[2], 16) == int(p[3], 16) == int(p[4], 16) == 0 and
             ((dest.is_loopback and int(p[1], 16) == 128) or
              (int(p[0], 16) == 0 and int(p[1], 16) == 0 and int(p[8], 16) & 0x200)), "nonloopback-v6-route")
    sockets = 0
    for name in ("tcp", "tcp6", "udp", "udp6"):
        lines = raws[name].decode("ascii").splitlines()
        need(bool(lines) and "local_address" in lines[0] and
             any(name in lines[0].split() for name in ("rem_address", "remote_address")), "socket-table-header")
        width = 32 if name.endswith("6") else 8
        for line in lines[1:]:
            p = line.split()
            need(len(p) >= 10, "socket-row")
            for endpoint in p[1:3]:
                match = re.fullmatch("([0-9a-fA-F]{" + str(width) + "}):([0-9a-fA-F]{4})", endpoint)
                need(match is not None, "socket-endpoint")
                addr = bytes.fromhex(match.group(1))
                addr = b"".join(addr[i:i + 4][::-1] for i in range(0, len(addr), 4))
                address = ipaddress.ip_address(addr)
                port = int(match.group(2), 16)
                mapped = address.ipv4_mapped if isinstance(address, ipaddress.IPv6Address) else None
                need(address.is_loopback or (mapped is not None and mapped.is_loopback) or
                     (address.is_unspecified and port == 0), "nonloopback-socket")
            sockets += 1
    return {"scope": "bounded-nonatomic-proc-network-snapshot", "loopback_only_observed": True,
            "v4_local_leaves": leaves, "socket_rows": sockets,
            "v6_reject_defaults": "non-forwarding-lo-reject-only"}


def read_network(reader):
    raws = {name: reader("/proc/net/" + name, 65536) for name in NETWORK_FILES}
    result = network_values(raws)
    result["raw"] = {k: {"base64": base64.b64encode(v).decode(), "sha256": digest(v)} for k, v in raws.items()}
    return result


def verify_network(value):
    need(type(value) is dict and type(value.get("raw")) is dict and set(value["raw"]) == set(NETWORK_FILES), "network-proof-fields")
    raws = {}
    for name, proof in value["raw"].items():
        raw = base64.b64decode(proof["base64"], validate=True)
        need(base64.b64encode(raw).decode() == proof["base64"] and digest(raw) == proof["sha256"], "network-proof-raw")
        raws[name] = raw
    expected = network_values(raws)
    need({k: v for k, v in value.items() if k != "raw"} == expected, "network-proof-result")
    return expected


def verify_driver(lines, nonce, server_identity):
    """Validate original receipts;224 relies additionally on qualified driver control flow."""
    need(hex_value(nonce, 32) and hex_value(server_identity, 40), "driver-binding")
    need(type(lines) is list and len(lines) == 4, "driver-four-phases")
    phases = ("thirty-two-proven", "sixteen-survivors", "thirty-two-recovered", "zero-drained")
    ordinals = (list(range(1, 33)), list(range(17, 33)), list(range(17, 49)), [])
    expected_work = (32, 176, 224, 224)
    identities, identity_set, last_frames, last_bytes = {}, set(), 0, 0
    for index, value in enumerate(lines):
        row = strict_json(value) if type(value) is bytes else value
        need(type(row) is dict and set(row) == {"kind", "nonce", "phase", "verified_channels", "peers",
             "cached_live", "cached_live_acceptance", "application_charge"}, "driver-record-shape")
        need(row["kind"] == "c2b" and row["nonce"] == nonce and row["phase"] == phases[index] and
             type(row["verified_channels"]) is int and row["verified_channels"] == len(ordinals[index]) and
             row["cached_live_acceptance"] is False, "driver-phase-authority")
        peers = row["peers"]
        need(type(peers) is list or (index == 3 and peers is None), "driver-peers-type")
        peers = [] if peers is None else peers
        need([p.get("ordinal") if type(p) is dict else None for p in peers] == ordinals[index], "driver-ordinals")
        for peer in peers:
            need(set(peer) == {"ordinal", "identity", "server", "local", "remote", "useful_exchanges"}, "driver-peer-fields")
            number = peer["ordinal"]
            count = 1 if index == 0 else 6 if index == 1 else 7 if number <= 32 else 2
            need(type(number) is int and type(peer["useful_exchanges"]) is int and
                 peer["useful_exchanges"] == count and peer["server"] == server_identity and
                 hex_value(peer["identity"], 40) and type(peer["local"]) is str and
                 re.fullmatch(r"127\.0\.0\.1:([1-9][0-9]{0,4})", peer["local"]) is not None and
                 0 < int(peer["local"].rsplit(":", 1)[1]) <= 65535 and
                 peer["remote"] == "127.0.0.1:64646", "driver-peer-proof")
            if number in identities:
                need(identities[number] == peer["identity"], "driver-identity-changed")
            else:
                need(peer["identity"] not in identity_set and peer["identity"] != server_identity, "driver-identity-reuse")
                identities[number] = peer["identity"]
                identity_set.add(peer["identity"])
        charge = row["application_charge"]
        need(type(charge) is dict and set(charge) == {"frames", "bytes"} and
             all(type(charge[k]) is int for k in charge), "driver-charge-type")
        need(max(last_frames, 6 * expected_work[index]) <= charge["frames"] <= 2048 and
             last_bytes <= charge["bytes"] <= 8 * 1024**2, "driver-charge-bound-or-rollback")
        last_frames, last_bytes = charge["frames"], charge["bytes"]
    need(set(identities) == set(range(1, 49)) and len(identity_set) == 48, "driver-all-identities")
    return {"phases": [32, 16, 32, 0], "identities": 48, "source_bound_useful_sequences": 224,
            "source_bound_primary_pairs": 672, "full_terminal_per_peer_RAW": False,
            "frames": last_frames, "bytes": last_bytes}


def uint(value):
    need(type(value) is int and 0 <= value < (1 << 64), "unsigned-value")
    return value


def bracket(value):
    need(type(value) is list and len(value) == 2 and all(type(v) is int and 0 <= v < (1 << 63) for v in value)
         and value[0] <= value[1], "clock-bracket")
    return value


def namespace(value):
    need(type(value) is list and len(value) == 2 and all(type(v) is int and v > 0 for v in value), "namespace-identity")
    return value


def clock_values(value, nonce):
    need(type(value) is dict and value.get("schema") == "d1-clock-probe-v1" and value.get("nonce") == nonce and
         value.get("complete") is True and value.get("supported_source_guard") is True and
         value.get("shared_time_domain") is True and value.get("errors") == [], "clock-qualification")
    need(type(value.get("pid")) is type(value.get("native_tid")) is int and
         0 < value["pid"] < (1 << 31) and 0 < value["native_tid"] < (1 << 31), "clock-owner-types")
    rows = value["contexts"]
    need(type(rows) is dict and {"self", "thread_self", "numeric_pid"}.issubset(rows) and
         set(rows).issubset({"self", "thread_self", "numeric_pid", "target_keeper"}), "clock-contexts")
    domain = namespace(rows["thread_self"]["time"])
    for name, row in rows.items():
        need(namespace(row["time"]) == namespace(row["time_for_children"]) == domain, "clock-domain-mismatch")
        need(type(row["process"]) is dict and set(row["process"]) == {"pid", "ppid", "start_ticks"} and
             all(type(v) is int for v in row["process"].values()) and
             0 < row["process"]["pid"] < (1 << 31) and 0 <= row["process"]["ppid"] < (1 << 31) and
             row["process"]["start_ticks"] > 0 and
             row["status"]["Uid"] == [501] * 4 and row["status"]["Gid"] == [20] * 4,
             "clock-process-binding")
        status = row["status"]
        need(type(status) is dict and set(status) == {"Pid", "Tgid", "NSpid", "Uid", "Gid"} and
             all(type(v) is list and all(type(x) is int for x in v) for v in status.values()) and
             len(status["Pid"]) == len(status["Tgid"]) == 1 and 1 <= len(status["NSpid"]) <= 32,
             "clock-status-types")
        offset = row["offsets_for_children"]
        raw = base64.b64decode(offset["raw_base64"], validate=True)
        need(0 < len(raw) <= 1024 and base64.b64encode(raw).decode() == offset["raw_base64"] and
             digest(raw) == offset["sha256"], "clock-offset-raw")
        text = raw.decode("ascii")
        need(text.endswith("\n") and "\r" not in text, "clock-offset-format")
        parsed = {}
        for line in text.splitlines():
            parts = line.split()
            need(len(parts) == 3 and parts[0] in ("monotonic", "boottime") and parts[0] not in parsed and
                 re.fullmatch(r"-?[0-9]{1,20}", parts[1]) is not None and
                 re.fullmatch(r"[0-9]{1,20}", parts[2]) is not None, "clock-offset-fields")
            sec, nano = int(parts[1]), int(parts[2])
            need(-(1 << 63) <= sec < (1 << 63) and 0 <= nano < 1000000000, "clock-offset-range")
            parsed[parts[0]] = {"seconds": sec, "nanoseconds": nano}
        need(set(parsed) == {"monotonic", "boottime"} and type(offset.get("values")) is dict and
             all(type(v) is dict and all(type(n) is int for n in v.values()) for v in offset["values"].values()) and
             offset["values"] == parsed, "clock-offset-values")
        if name in ("self", "numeric_pid"):
            need(row["process"]["pid"] == value["pid"], "clock-pid-view")
    thread = rows["thread_self"]
    proof = thread["offsets_reader"]
    link = proof["thread_self_link_before"]
    need(type(link) is str and proof["thread_self_link_after"] == link and
         re.fullmatch(r"[1-9][0-9]{0,9}/task/[1-9][0-9]{0,9}", link) is not None, "clock-thread-link")
    tgid, tid = (int(v) for v in link.split("/task/"))
    need(0 < tgid < (1 << 31) and 0 < tid < (1 << 31) and proof["path"] == "/proc/" + str(tid) and
         thread["process"]["pid"] == value["native_tid"] == tid and
         thread["status"]["Pid"] == [tid] and thread["status"]["Tgid"] == [tgid], "clock-thread-binding")
    expected = {k: thread[k] for k in ("process", "status", "time", "time_for_children")}
    need(proof["before"] == proof["after"] == expected, "clock-thread-bookends")
    for view in (proof["before"], proof["after"]):
        need(type(view) is dict and set(view) == set(expected) and type(view["process"]) is dict and
             all(type(v) is int for v in view["process"].values()) and type(view["status"]) is dict and
             all(type(v) is list and all(type(n) is int for n in v) for v in view["status"].values()),
             "clock-thread-proof-types")
        need(namespace(view["time"]) == namespace(view["time_for_children"]) == domain, "clock-proof-domain")
    bounds = bracket(value["own_boottime_bracket_ns"])
    need(type(value["own_boottime_observation_ns"]) is int and
         bounds[0] <= value["own_boottime_observation_ns"] <= bounds[1], "clock-observation")
    return domain


def facts_values(facts):
    need(type(facts) is dict, "facts-type")
    namespace(facts["cgroup_identity"])
    verify_network(facts["network"])
    need(type(facts["group_path"]) is str and facts["group_path"].startswith("/") and
         ".." not in facts["group_path"].split("/"), "group-path")
    read, peak, current = (bracket(facts[k]) for k in ("read_bracket_ns", "peak_bracket_ns", "current_bracket_ns"))
    need(read[0] <= peak[0] <= peak[1] <= current[0] <= current[1] <= read[1], "facts-bracket-order")
    need(uint(facts["peak"]) <= MEMORY, "peak-above-limit")
    uint(facts["current"])  # Not atomic with peak: deliberately no current<=peak assertion.
    limits = facts["limits"]
    need(type(limits) is dict and set(limits) == {"memory.max", "memory.swap.max", "memory.swap.current", "pids.max", "pids.current"}
         and all(type(v) is int for v in limits.values()) and limits["memory.max"] == MEMORY and
         limits["memory.swap.max"] == limits["memory.swap.current"] == 0 and limits["pids.max"] == 64 and
         0 <= limits["pids.current"] <= 64, "kernel-limits")
    cpu = facts["cpu_max"]
    need(type(cpu) is list and len(cpu) == 2 and type(cpu[0]) is type(cpu[1]) is int and cpu[0] == cpu[1] > 0,
         "kernel-cpu-limit")
    counters = {"peak": facts["peak"]}
    before, after = facts["events_before"], facts["events_after"]
    need(type(before) is type(after) is dict and set(before) == set(after) and len(before) <= 64 and
         {"low", "high", "max", "oom", "oom_kill", "oom_group_kill"}.issubset(before), "oom-fields")
    for key in before:
        need(uint(after[key]) >= uint(before[key]), "events-regressed-within-read")
        counters["events." + key] = after[key]
    need(all(before[k] == after[k] == 0 for k in ("oom", "oom_kill", "oom_group_kill")), "oom-event")
    for group, required in (("cpu_stat", {"usage_usec", "user_usec", "system_usec"}), ("pids_events", {"max"})):
        values = facts[group]
        need(type(values) is dict and required.issubset(values) and len(values) <= 64, "counter-fields")
        counters.update({group + "." + k: uint(v) for k, v in values.items()})
    members = facts["members"]
    need(type(members) is list and 1 <= len(members) <= 64 and
         all(type(p) is int and 0 < p < (1 << 31) for p in members) and members == sorted(set(members)), "group-members")
    return counters


def process_values(row, known, domain, allow_missing=False):
    need(type(row) is dict and type(row.get("pid")) is int and row["pid"] in known, "unknown-process")
    pid = row["pid"]
    process = row["process"]
    need(type(process) is dict and set(process) == {"pid", "ppid", "start_ticks"} and
         all(type(v) is int for v in process.values()) and process["pid"] == pid and
         process["start_ticks"] > 0 and process["ppid"] == (0 if pid == 1 else 1), "process-birth-parent")
    need(type(row.get("available")) is bool, "process-availability")
    if not row["available"]:
        need(allow_missing and row.get("reason") == "announced-exit-pending-owned-reap" and
             known[pid] is not None and known[pid]["process"] == process, "unexpected-process-loss")
        return
    status = row["status"]
    need(type(status) is dict and set(status) == {"Pid", "Tgid", "NSpid", "Uid", "Gid"} and
         all(type(v) is list and all(type(x) is int for x in v) for v in status.values()) and
         1 <= len(status["NSpid"]) <= 32 and all(0 < p < (1 << 31) for p in status["NSpid"]) and
         status.get("Pid") == [pid] and status.get("Tgid") == [pid] and
         status.get("Uid") == [501] * 4 and status.get("Gid") == [20] * 4 and
         namespace(row["namespace"]) == domain, "process-status-or-domain")
    namespace(row["executable_identity"])
    identity = {k: row[k] for k in ("process", "status", "namespace", "executable_identity")}
    if known[pid] is None:
        known[pid] = identity
    else:
        need(known[pid] == identity, "process-incarnation-changed")


def verify_secret_attachment(record, nonce):
    need(type(record) is dict and record == {"kind": "SECRET_ATTACHED", "nonce": nonce},
         "secret-attachment-receipt")


def verify_service(records, nonce, cli_sha):
    need(type(records) is list and 1 <= len(records) <= 4096, "service-record-count")
    allowed = {"SECRET_ATTACHED", "SAMPLER_READY", "SAMPLE", "NODE_BOUND", "READY", "STOPPING_ACK", "SAMPLER_FINAL",
               "SAMPLER_SEALED", "STATE_CHECK", "LOG", "FINAL", "ERROR"}
    selected = {}
    samples, states = [], []
    for row in records:
        need(type(row) is dict and row.get("nonce") == nonce and row.get("kind") in allowed, "service-record-binding")
        kind = row["kind"]
        need(kind != "ERROR", "service-error-record")
        if kind == "LOG":
            raw = base64.b64decode(row["raw_base64"], validate=True)
            need(type(row["bytes"]) is int and row["bytes"] == len(raw) <= 16384 and
                 row["stream"] in ("out", "err") and digest(raw) == row["sha256"] and
                 base64.b64encode(raw).decode() == row["raw_base64"], "node-log-frame")
        elif kind == "SAMPLE":
            samples.append(row)
        elif kind == "STATE_CHECK":
            states.append(row)
        else:
            need(kind not in selected, "duplicate-service-control")
            selected[kind] = row
    need(set(selected) == allowed - {"SAMPLE", "STATE_CHECK", "LOG", "ERROR"}, "missing-service-control")
    positions = {row["kind"]: index for index, row in enumerate(records) if row["kind"] in selected}
    order = ["SECRET_ATTACHED", "SAMPLER_READY", "NODE_BOUND", "READY", "STOPPING_ACK", "SAMPLER_FINAL", "SAMPLER_SEALED", "FINAL"]
    need([positions[k] for k in order] == sorted(positions[k] for k in order), "service-control-order")
    need(all(positions["READY"] < index < positions["STOPPING_ACK"] for index, row in enumerate(records)
             if row["kind"] == "STATE_CHECK"), "state-check-order")
    verify_secret_attachment(selected["SECRET_ATTACHED"], nonce)
    need(positions["SECRET_ATTACHED"] == 0, "secret-attachment-not-first")
    initial, final = selected["SAMPLER_READY"], selected["FINAL"]
    need(initial["ordinal"] == 0 and type(initial["ordinal"]) is int and bool(samples), "sampler-start-or-empty")
    ready = selected["READY"]
    need(hex_value(ready["identity"], 40) and type(ready["started_at"]) is str and ready["started_at"].endswith("Z"), "node-ready-binding")
    clocks = [initial["clock"], ready["clock"], selected["SAMPLER_SEALED"]["clock"], final["clock"]]
    domains = [clock_values(c, nonce) for c in clocks]
    need(all(d == domains[0] for d in domains), "service-clock-domains")
    sampler_pid = initial["clock"]["pid"]
    for clock, pid, role, phase in zip(clocks, (sampler_pid, 1, sampler_pid, 1),
            ("observer", "keeper", "observer", "keeper"),
            ("sg32-sampler-before", "sg32-keeper-before", "sg32-sampler-after", "sg32-keeper-after")):
        need(type(clock["pid"]) is int and clock["pid"] == pid and clock["role"] == role and
             clock["phase"] == phase, "clock-owner-or-phase")
    bound = selected["NODE_BOUND"]
    node_pid = bound["pid"]
    need(type(sampler_pid) is type(node_pid) is int and len({1, sampler_pid, node_pid}) == 3,
         "owned-distinct-processes")
    known = {1: None, sampler_pid: None, node_pid: None}
    for index, record in enumerate(records):
        if record["kind"] not in ("SAMPLER_READY", "SAMPLE", "SAMPLER_FINAL"):
            continue
        processes = record["processes"]
        need(type(processes) is list and len(processes) == len({p["pid"] for p in processes}), "process-list")
        expected = {1, sampler_pid} if record["kind"] in ("SAMPLER_READY", "SAMPLER_FINAL") else set(known)
        need({p["pid"] for p in processes} == expected, "missing-process-witness")
        for process in processes:
            process_values(process, known, domains[0], allow_missing=(process["pid"] == node_pid and
                           positions["STOPPING_ACK"] < index < positions["SAMPLER_FINAL"]))
        members = record["facts"]["members"]
        need(set(members).issubset(known) and {1, sampler_pid}.issubset(members), "membership-unknown-or-missing")
        if record["kind"] == "SAMPLE" and index < positions["STOPPING_ACK"]:
            need(set(members) == set(known), "live-node-outside-group")
    need(known[node_pid] is not None and known[node_pid]["process"]["start_ticks"] == bound["start_ticks"] and
         selected["STOPPING_ACK"]["pid"] == node_pid and
         selected["STOPPING_ACK"]["start_ticks"] == bound["start_ticks"], "stopping-owned-birth")
    process_values(final["keeper_process"], known, domains[0])
    facts = [ready["initial"], initial["facts"]] + [x["facts"] for x in samples] + [selected["SAMPLER_FINAL"]["facts"], final["facts"]]
    facts.sort(key=lambda f: f["read_bracket_ns"][0])
    previous, keyset = {}, None
    for fact in facts:
        current = facts_values(fact)
        if keyset is not None:
            need(set(current) == keyset and all(current[k] >= previous[k] for k in current), "kernel-counter-rollback")
        need(fact["cgroup_identity"] == facts[0]["cgroup_identity"] and fact["group_path"] == facts[0]["group_path"], "cgroup-incarnation-changed")
        previous, keyset = current, set(current)
    sequence = [initial] + samples + [selected["SAMPLER_FINAL"]]
    need([x["ordinal"] for x in sequence] == list(range(len(sequence))) and
         all(type(x["ordinal"]) is int for x in sequence), "sampler-sequence-gap")
    times = [x["facts"]["read_bracket_ns"] for x in sequence]
    for before, after in zip(times, times[1:]):
        need(before[1] <= after[0], "sampler-read-order")
    reap = selected["SAMPLER_FINAL"]["node_reap"]
    need(type(reap.get("at_ns")) is int and reap.get("nonce") == nonce and reap.get("kind") == "REAP", "reap-framing")
    prior_ends = [b[1] for b in times if b[1] <= reap["at_ns"]]
    need(bool(prior_ends), "missing-reap-coverage-edge")
    edge = max(prior_ends)
    need(type(reap["exit"]) is int and reap["exit"] == 0 and reap["pid"] == bound["pid"] and
         reap["start_ticks"] == bound["start_ticks"] and type(reap["at_ns"]) is int and
         reap["at_ns"] <= times[-1][1] and reap["at_ns"] - edge <= 2000000000 and
         selected["SAMPLER_FINAL"]["node_reap_edge_end_ns"] == edge, "owned-reap-or-final-edge")
    for before, after in zip(times, times[1:]):
        if after[1] <= reap["at_ns"]:
            need(after[1] - before[1] <= 2000000000, "sampler-coverage-gap")
    node_final = selected["SAMPLER_FINAL"]["node_final"]
    need(node_final["exe_sha256"] == cli_sha and node_final["pid"] == bound["pid"] and
         node_final["process"]["start_ticks"] == bound["start_ticks"] and node_final["namespace"] == domains[0], "node-final-executable")
    need(type(final["node_exit"]) is type(final["sampler_exit"]) is int and final["node_exit"] == final["sampler_exit"] == 0 and
         final["facts"]["members"] == [1] and final["keeper_exit_tail"] == "UNKNOWN", "final-owned-seal")
    need(ready["initial"]["members"] == [1] and ready["initial"]["initial"] is True and
         initial["facts"]["initial"] is True and final["facts"]["initial"] is False, "initial-final-cgroup-scope")
    phases = ["thirty-two-proven", "sixteen-survivors", "thirty-two-recovered", "zero-drained"]
    need([x["phase"] for x in states] == phases and final["checked_phases"] == phases, "state-phase-checks")
    for check in states:
        need(type(check["bytes"]) is int and 0 <= check["bytes"] <= 64 * 1024**2 and
             type(check["entries"]) is int and 0 <= check["entries"] <= 4096, "state-size-limit")
        bracket(check["bracket_ns"])
    for check in (final["state"],):
        need(type(check["bytes"]) is int and 0 <= check["bytes"] <= 64 * 1024**2 and
             type(check["entries"]) is int and 0 <= check["entries"] <= 4096, "final-state-size")
        bracket(check["bracket_ns"])
    return {"peak_bytes": final["facts"]["peak"], "current_bytes": final["facts"]["current"],
            "peak_bracket_ns": final["facts"]["peak_bracket_ns"], "samples": len(sequence),
            "kernel_window_end": "bounded-final-peak-read", "keeper_exit_tail": "UNKNOWN",
            "isolated_role_charge": "UNKNOWN", "physical_shared_pages": "UNKNOWN"}
