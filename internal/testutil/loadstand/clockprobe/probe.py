"""Finite namespace/clock facts. Import is inert; no namespace mutation."""
import base64
import hashlib
import json
import os
import re
import sys
import threading
import time

ROLE_MAX = 65536
SCHEMA = "d1-clock-probe-v1"


class Fault(Exception):
    pass


def need(value, reason):
    if not value:
        raise Fault(reason)


def read_file(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        raw = bytearray()
        while True:
            part = os.read(fd, min(4096, limit + 1 - len(raw)))
            if not part:
                break
            raw.extend(part)
            need(len(raw) <= limit, "read-limit")
        return bytes(raw)
    finally:
        os.close(fd)


def number(text):
    need(re.fullmatch(r"[0-9]{1,20}", text) is not None, "integer-format")
    value = int(text)
    need(value < (1 << 64), "integer-range")
    return value


def proc_stat(raw):
    text = raw.decode("ascii")
    left, right = text.find("("), text.rfind(")")
    need(left > 0 and right > left, "stat-format")
    tail = text[right + 1:].split()
    need(len(tail) >= 20, "stat-fields")
    value = {"pid": number(text[:left].strip()), "ppid": number(tail[1]),
             "start_ticks": number(tail[19])}
    need(0 < value["pid"] < (1 << 31) and value["ppid"] < (1 << 31), "stat-pid-range")
    return value


def proc_status(raw):
    fields = {}
    for line in raw.decode("ascii").splitlines():
        name, sep, value = line.partition(":")
        if sep and name in ("Pid", "Tgid", "NSpid", "Uid", "Gid"):
            need(name not in fields, "status-duplicate")
            fields[name] = [number(v) for v in value.split()]
    need(set(fields) == {"Pid", "Tgid", "NSpid", "Uid", "Gid"}, "status-missing")
    need(len(fields["Pid"]) == len(fields["Tgid"]) == 1 and
         1 <= len(fields["NSpid"]) <= 32 and
         len(fields["Uid"]) == len(fields["Gid"]) == 4, "status-shape")
    need(all(0 < p < (1 << 31) for key in ("Pid", "Tgid", "NSpid") for p in fields[key]) and
         all(v < (1 << 32) for key in ("Uid", "Gid") for v in fields[key]), "status-range")
    return fields


def parse_offsets(raw):
    text = raw.decode("ascii")
    need(text.endswith("\n") and "\r" not in text, "offset-format")
    values = {}
    for line in text.splitlines():
        parts = line.split()
        need(len(parts) == 3 and parts[0] in ("monotonic", "boottime") and
             parts[0] not in values, "offset-fields")
        need(re.fullmatch(r"-?[0-9]{1,20}", parts[1]) is not None, "offset-seconds")
        sec, nano = int(parts[1]), number(parts[2])
        need(-(1 << 63) <= sec < (1 << 63) and nano < 1000000000, "offset-range")
        values[parts[0]] = {"seconds": sec, "nanoseconds": nano}
    need(set(values) == {"monotonic", "boottime"}, "offset-missing")
    return values


def ns_identity(fd):
    stat = os.fstat(fd)
    need(type(stat.st_dev) is int and type(stat.st_ino) is int and
         0 <= stat.st_dev < (1 << 64) and 0 < stat.st_ino < (1 << 64), "namespace-identity-range")
    return [stat.st_dev, stat.st_ino]


def boottime():
    need(hasattr(time, "CLOCK_BOOTTIME") and hasattr(time, "clock_gettime_ns"),
         "boottime-unsupported")
    value = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    need(type(value) is int and 0 <= value < (1 << 63), "boottime-invalid")
    return value


def proc_mounts(raw):
    rows = []
    for line in raw.decode("ascii").splitlines():
        before, sep, after = line.partition(" - ")
        need(not sep or bool(after.split()), "mount-fields")
        if sep and after.split()[0] == "proc":
            need(len(rows) < 16 and len(line) <= 2048, "proc-mount-limit")
            rows.append(line)
    need(bool(rows), "proc-mount-missing")
    return {"sha256": hashlib.sha256(raw).hexdigest(), "proc_lines": rows}


def thread_reader(prefix, held=None):
    row = {"process": proc_stat(read_file(prefix + "/stat", 8192)),
           "status": proc_status(read_file(prefix + "/status", 16384))}
    for kind in ("time", "time_for_children"):
        path = prefix + "/ns/" + kind
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
        if held is not None:
            held.append(("thread_self", kind, path, fd))
        try:
            row[kind] = ns_identity(fd)
        finally:
            if held is None:
                os.close(fd)
    return row


def thread_offsets_path(link):
    need(type(link) is str, "thread-link-type")
    match = re.fullmatch(r"([1-9][0-9]{0,9})/task/([1-9][0-9]{0,9})", link)
    need(match is not None, "thread-link-format")
    tgid, tid = (int(v) for v in match.groups())
    need(tgid < (1 << 31) and tid < (1 << 31), "thread-link-range")
    return "/proc/" + str(tid), tgid, tid


def capture(role, phase, nonce):
    pid, tid = os.getpid(), threading.get_native_id()
    aliases = {"self": "/proc/self", "thread_self": "/proc/thread-self",
               "numeric_pid": "/proc/" + str(pid)}
    if role == "observer":
        aliases["target_keeper"] = "/proc/1"
    packet = {"schema": SCHEMA, "nonce": nonce, "role": role, "phase": phase,
              "pid": pid, "native_tid": tid, "complete": False,
              "shared_time_domain": None, "supported_source_guard": None,
              "qualification": "UNKNOWN", "contexts": {}, "errors": []}
    held = []
    try:
        need(type(pid) is int and type(tid) is int and 0 < pid < (1 << 31) and
             0 < tid < (1 << 31), "actor-id-range")
        for name, prefix in aliases.items():
            row = packet["contexts"][name] = {"path": prefix}
            row["process"] = proc_stat(read_file(prefix + "/stat", 8192))
            row["status"] = proc_status(read_file(prefix + "/status", 16384))
            for kind in ("time", "time_for_children"):
                path = prefix + "/ns/" + kind
                fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
                held.append((name, kind, path, fd))
                row[kind] = ns_identity(fd)
                link = os.readlink(path)
                need(len(link) <= 128, "namespace-link-limit")
                row[kind + "_readlink"] = link
        packet["mounts"] = proc_mounts(read_file("/proc/self/mountinfo", 65536))
        thread = packet["contexts"]["thread_self"]
        link = os.readlink("/proc/thread-self")
        reader_path, proc_tgid, proc_tid = thread_offsets_path(link)
        need(thread["process"]["pid"] == proc_tid and thread["status"]["Pid"] == [proc_tid] and
             thread["status"]["Tgid"] == [proc_tgid], "thread-reader-pid-binding")
        expected_thread = {k: thread[k] for k in ("process", "status", "time", "time_for_children")}
        proof = thread["offsets_reader"] = {"path": reader_path, "thread_self_link_before": link,
                                           "before": thread_reader(reader_path, held)}
        need(proof["before"] == expected_thread, "thread-reader-before-binding")
        offset_paths = {name: (reader_path if name == "thread_self" else prefix)
                        for name, prefix in aliases.items()}
        before, initial_offsets = boottime(), {}
        for name, prefix in offset_paths.items():
            raw = read_file(prefix + "/timens_offsets", 1024)
            initial_offsets[name] = raw
            packet["contexts"][name]["offsets_for_children"] = {
                "raw_base64": base64.b64encode(raw).decode("ascii"),
                "sha256": hashlib.sha256(raw).hexdigest(), "values": parse_offsets(raw)}
        observation = boottime()
        for name, prefix in offset_paths.items():
            need(read_file(prefix + "/timens_offsets", 1024) == initial_offsets[name], "offsets-changed")
        after = boottime()
        proof["thread_self_link_after"] = os.readlink("/proc/thread-self")
        proof["after"] = thread_reader(reader_path)
        need(proof["thread_self_link_after"] == link and proof["after"] == expected_thread,
             "thread-reader-after-binding")
        need(before <= observation <= after, "boottime-regression")
        packet["own_boottime_observation_ns"] = observation
        packet["own_boottime_bracket_ns"] = [before, after]
        for name, kind, path, fd in held:
            need(ns_identity(fd) == packet["contexts"][name][kind], "held-namespace-changed")
            fresh = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            try:
                need(ns_identity(fresh) == ns_identity(fd), "named-namespace-changed")
            finally:
                os.close(fresh)
        for name, prefix in aliases.items():
            row = packet["contexts"][name]
            need(proc_stat(read_file(prefix + "/stat", 8192)) == row["process"], "process-changed")
            need(proc_status(read_file(prefix + "/status", 16384)) == row["status"], "status-changed")
        rows = packet["contexts"]
        own = [rows[n]["time"] for n in ("self", "thread_self", "numeric_pid")]
        packet["own_aliases_equal"] = own[0] == own[1] == own[2]
        packet["actor_pid_view_matches"] = (all(rows[n]["status"]["Tgid"] == [pid]
            for n in ("self", "thread_self", "numeric_pid")) and
            all(rows[n]["process"]["pid"] == pid and rows[n]["status"]["Pid"] == [pid]
                for n in ("self", "numeric_pid")) and
            rows["thread_self"]["process"]["pid"] == tid and rows["thread_self"]["status"]["Pid"] == [tid])
        packet["current_equals_for_children"] = {n: r["time"] == r["time_for_children"] for n, r in rows.items()}
        target = rows["target_keeper"]["time"] if role == "observer" else own[0]
        packet["old_numeric_comparator_equal"] = own[2] == target
        packet["actor_current_equals_target_current"] = own[1] == target
        packet["supported_source_guard"] = (packet["own_aliases_equal"] and
            packet["actor_pid_view_matches"] and packet["actor_current_equals_target_current"] and
            all(packet["current_equals_for_children"].values()))
        packet["shared_time_domain"] = packet["supported_source_guard"]
        packet["source_refusal"] = None if packet["supported_source_guard"] else "clock-domain-or-binding-unsupported"
        packet["complete"] = True
    except (OSError, ValueError, Fault, UnicodeError) as error:
        packet["errors"].append({"type": type(error).__name__, "reason": str(error)[:160]})
    finally:
        for _, _, _, fd in reversed(held):
            try:
                os.close(fd)
            except OSError:
                packet["complete"] = False
                packet["errors"].append({"reason": "held-fd-close-failed"})
        if not packet["complete"]:
            packet["supported_source_guard"] = packet["shared_time_domain"] = None
            packet["source_refusal"] = "UNKNOWN"
    return packet


def emit(packet):
    raw = json.dumps(packet, separators=(",", ":"), allow_nan=False).encode("ascii") + b"\n"
    need(len(raw) <= ROLE_MAX // 2, "packet-output-limit")
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    need(len(args) == 2 and args[0] in ("keeper", "observer") and
         re.fullmatch(r"[0-9a-f]{32}", args[1]) is not None, "arguments")
    role, nonce = args
    if role == "observer":
        first = capture(role, "observer", nonce)
        emit(first)
        return 0 if first["complete"] else 2
    lifetime, identities = [], {}
    try:
        for name, prefix in (("self", "/proc/self"), ("thread_self", "/proc/thread-self"),
                             ("numeric_pid", "/proc/" + str(os.getpid()))):
            identities[name] = {}
            for kind in ("time", "time_for_children"):
                fd = os.open(prefix + "/ns/" + kind, os.O_RDONLY | os.O_CLOEXEC)
                lifetime.append((name, kind, fd))
                identities[name][kind] = ns_identity(fd)
        first = capture(role, "keeper-ready", nonce)
        first["lifetime_held_before"] = identities
        if any(first["contexts"].get(name, {}).get(kind) != identities[name][kind]
               for name, kind, _ in lifetime):
            first["complete"] = False
            first["supported_source_guard"] = first["shared_time_domain"] = None
            first["source_refusal"] = "UNKNOWN"
            first["errors"].append({"reason": "keeper-initial-fd-binding"})
        emit(first)
        request = sys.stdin.buffer.readline(6)
        if request not in (b"STOP\n", b""):
            return 2
        final = capture(role, "keeper-stop", nonce)
        after = {name: {} for name in identities}
        for name, kind, fd in lifetime:
            after[name][kind] = ns_identity(fd)
        final["lifetime_held_after"] = after
        if (request != b"STOP\n" or after != identities or
                any(final["contexts"].get(name, {}).get(kind) != identities[name][kind]
                    for name, kind, _ in lifetime)):
            final["complete"] = False
            final["supported_source_guard"] = final["shared_time_domain"] = None
            final["source_refusal"] = "UNKNOWN"
            final["errors"].append({"reason": "keeper-lifetime-fd-changed"})
        for _, _, fd in reversed(lifetime):
            try:
                os.close(fd)
            except OSError:
                final["complete"] = False
                final["supported_source_guard"] = final["shared_time_domain"] = None
                final["source_refusal"] = "UNKNOWN"
                final["errors"].append({"reason": "keeper-lifetime-close-failed"})
        lifetime.clear()
        emit(final)
        return 0 if first["complete"] and final["complete"] else 2
    finally:
        for _, _, fd in reversed(lifetime):
            os.close(fd)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, Fault) as error:
        sys.stderr.write(type(error).__name__ + ":" + str(error)[:160] + "\n")
        raise SystemExit(2)
