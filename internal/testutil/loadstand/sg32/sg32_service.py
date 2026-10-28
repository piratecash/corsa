"""SG32 service-side preparation only. Import performs no process or filesystem work."""
import base64
import contextlib
import datetime
import errno
import hashlib
import http.client
import json
import os
from pathlib import PurePosixPath
import re
import secrets
import selectors
import signal
import stat
import subprocess
import sys
import time

from sg32_common import Fault, need, strict_json, encode, read_network
import clockprobe

INPUTS = ("corsad", "c2driver", "sg32_common.py", "sg32_service.py", "clockprobe.py")
MEMORY, PID_LIMIT, RAW_LIMIT = 512000000, 64, 16 * 1024 * 1024
CADENCE_NS, GAP_NS = 500000000, 2000000000
PHASES = ("thirty-two-proven", "sixteen-survivors", "thirty-two-recovered", "zero-drained")
EVENTS = {"low", "high", "max", "oom", "oom_kill", "oom_group_kill"}
OOM = ("oom", "oom_kill", "oom_group_kill")
OUTPUT = 0


class Adverse(Fault):
    def __init__(self, reason, facts=None):
        super().__init__(reason)
        self.facts = facts


def uint(raw):
    need(type(raw) is bytes and re.fullmatch(rb"(?:0|[1-9][0-9]{0,19})\n?", raw) is not None,
         "unsigned-decimal")
    value = int(raw)
    need(value < (1 << 64), "unsigned-range")
    return value


def kv(raw, required):
    need(type(raw) is bytes and len(raw) <= 8192 and raw.endswith(b"\n"), "counter-framing")
    values = {}
    for line in raw.splitlines():
        parts = line.split(b" ")
        need(len(parts) == 2 and re.fullmatch(rb"[a-z][a-z0-9_]{0,40}", parts[0]) is not None,
             "counter-format")
        key = parts[0].decode("ascii")
        need(key not in values and len(values) < 32, "counter-duplicate-or-limit")
        values[key] = uint(parts[1])
    need(set(required) <= set(values), "counter-missing")
    return values


def safe_path(text):
    need(type(text) is str and 0 < len(text) <= 4096 and text.startswith("/") and
         "\\" not in text and "\x00" not in text, "absolute-path")
    need(text == "/" or all(x not in (".", "..", "") for x in text.split("/")[1:]),
         "absolute-path-components")
    need(str(PurePosixPath(text)) == text, "canonical-path")
    return text


def cg_path(raw, mount_raw):
    need(type(raw) is bytes and len(raw) <= 4096 and raw.count(b"\n") == 1 and raw.startswith(b"0::"),
         "unified-cgroup-required")
    group = safe_path(raw[3:-1].decode("ascii"))
    found = []
    for line in mount_raw.decode("ascii").splitlines():
        left, sep, right = line.partition(" - ")
        fields, tail = left.split(), right.split()
        if sep and tail and tail[0] == "cgroup2":
            need(len(fields) >= 6 and len(tail) >= 3, "cgroup-mount-fields")
            root, mount = safe_path(fields[3]), safe_path(fields[4])
            if group == root or root == "/" or group.startswith(root + "/"):
                found.append((root, mount))
    need(len(found) == 1, "one-cgroup-mount-required")
    root, mount = found[0]
    suffix = group[len(root):] if root != "/" else group[1:]
    path = mount.rstrip("/") + ("/" + suffix.lstrip("/") if suffix else "")
    return path, group


def stamp(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def close_fds(fds):
    failure = None
    for fd in reversed(fds):
        try:
            os.close(fd)
        except OSError as error:
            failure = error
    if failure is not None:
        raise failure


def identity(fd):
    return clockprobe.ns_identity(fd)


def read_fd(fd, limit):
    os.lseek(fd, 0, os.SEEK_SET)
    result = bytearray()
    while True:
        chunk = os.read(fd, min(65536, limit + 1 - len(result)))
        if not chunk:
            return bytes(result)
        result.extend(chunk)
        need(len(result) <= limit, "file-read-limit")


def file_hash(fd, limit):
    original = stamp(os.fstat(fd))
    need(0 < original[6] <= limit, "executable-size")
    os.lseek(fd, 0, os.SEEK_SET)
    digest, total = hashlib.sha256(), 0
    while True:
        part = os.read(fd, 65536)
        if not part:
            break
        total += len(part)
        need(total <= limit, "hash-limit")
        digest.update(part)
    need(total == original[6] and stamp(os.fstat(fd)) == original, "hashed-file-changed")
    return digest.hexdigest()


def emit(value):
    global OUTPUT
    raw = encode(value, 65536)
    need(OUTPUT + len(raw) <= RAW_LIMIT, "service-output-limit")
    OUTPUT += len(raw)
    need(sys.stdout.buffer.write(raw) == len(raw), "stdout-short-write")
    sys.stdout.buffer.flush()


def qualified_clock(role, phase, nonce):
    value = clockprobe.capture(role, phase, nonce)
    need(value.get("complete") is True and value.get("supported_source_guard") is True and
         value.get("shared_time_domain") is True, "clock-domain-unqualified")
    return value


class Cgroup:
    NAMES = ("memory.max", "memory.current", "memory.peak", "memory.swap.max", "memory.swap.current",
             "memory.events", "cpu.max", "cpu.stat", "pids.max", "pids.current", "pids.events", "cgroup.procs")

    def __init__(self):
        self.fds, self.last, self.closed = {}, {}, False
        raw = clockprobe.read_file("/proc/self/cgroup", 4096)
        self.path, self.group = cg_path(raw, clockprobe.read_file("/proc/self/mountinfo", 65536))
        self.raw = raw
        self.directory = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        self.directory_id = identity(self.directory)
        try:
            for name in self.NAMES:
                self.fds[name] = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.directory)
            self.ids = {name: identity(fd) for name, fd in self.fds.items()}
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, _type, error, _trace):
        try:
            self.close()
        except OSError:
            if error is None:
                raise
            error.cleanup_failed = True

    def close(self):
        if not self.closed:
            self.closed = True
            close_fds(list(self.fds.values()) + [self.directory])

    def check(self):
        need(identity(self.directory) == self.directory_id and
             clockprobe.read_file("/proc/self/cgroup", 4096) == self.raw, "cgroup-identity-changed")
        fresh = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            need(identity(fresh) == self.directory_id, "cgroup-name-changed")
        finally:
            os.close(fresh)
        for name, fd in self.fds.items():
            need(identity(fd) == self.ids[name], "cgroup-metric-fd-changed")
            current = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.directory)
            try:
                need(identity(current) == self.ids[name], "cgroup-metric-name-changed")
            finally:
                os.close(current)

    def snapshot(self, initial=False):
        self.check()
        begin = clockprobe.boottime()
        before = kv(read_fd(self.fds["memory.events"], 8192), EVENTS)
        partial = {"scope": "INCOMPLETE_CGROUP_PREFIX", "complete": False,
                   "cgroup_identity": self.directory_id, "initial": initial,
                   "read_begin_ns": begin, "events_before": before}
        if any(before[k] != 0 for k in OOM):
            raise Adverse("observed-oom-prefix", partial)
        peak_begin = clockprobe.boottime()
        peak = uint(read_fd(self.fds["memory.peak"], 64))
        peak_end = clockprobe.boottime()
        partial.update(peak=peak, peak_bracket_ns=[peak_begin, peak_end])
        if peak > MEMORY:
            raise Adverse("observed-peak-limit-prefix", partial)
        after = kv(read_fd(self.fds["memory.events"], 8192), EVENTS)
        partial["events_after"] = after
        if any(after[k] != 0 for k in OOM):
            raise Adverse("observed-oom-prefix", partial)
        current_begin = clockprobe.boottime()
        current = uint(read_fd(self.fds["memory.current"], 64))
        current_end = clockprobe.boottime()
        scalars = {key: uint(read_fd(self.fds[key], 64)) for key in
                   ("memory.max", "memory.swap.max", "memory.swap.current", "pids.max", "pids.current")}
        cpu = read_fd(self.fds["cpu.max"], 128).split()
        need(len(cpu) == 2 and uint(cpu[0]) == uint(cpu[1]) and uint(cpu[0]) > 0, "cpu-limit-not-one")
        cpu_stat = kv(read_fd(self.fds["cpu.stat"], 8192), {"usage_usec", "user_usec", "system_usec"})
        pid_events = kv(read_fd(self.fds["pids.events"], 8192), {"max"})
        members_raw = read_fd(self.fds["cgroup.procs"], 4096)
        members = [uint(v) for v in members_raw.splitlines()]
        need(1 <= len(members) <= 64 and all(0 < v < (1 << 31) for v in members) and
             len(members) == len(set(members)), "cgroup-members-format")
        need(scalars["memory.max"] == MEMORY and scalars["memory.swap.max"] == 0 and
             scalars["memory.swap.current"] == 0 and scalars["pids.max"] == PID_LIMIT and
             scalars["pids.current"] <= PID_LIMIT, "cgroup-limit-mismatch")
        counters = {"peak": peak, **{"events." + k: v for k, v in before.items()},
                    **{"after." + k: v for k, v in after.items()},
                    **{"cpu." + k: v for k, v in cpu_stat.items()},
                    **{"pids." + k: v for k, v in pid_events.items()}}
        need(set(before) == set(after) and (not self.last or set(counters) == set(self.last)) and
             all(k not in self.last or v >= self.last[k] for k, v in counters.items()) and
             all(after[k] >= before[k] >= self.last.get("after." + k, 0) for k in before),
             "counter-regression")
        self.last = counters
        self.check()
        network = read_network(clockprobe.read_file)
        end = clockprobe.boottime()
        need(begin <= peak_begin <= peak_end <= current_begin <= current_end <= end, "sample-clock-regression")
        result = {"cgroup_identity": self.directory_id, "group_path": self.group,
                  "read_bracket_ns": [begin, end], "peak_bracket_ns": [peak_begin, peak_end],
                  "current_bracket_ns": [current_begin, current_end], "peak": peak, "current": current,
                  "events_before": before, "events_after": after, "limits": scalars,
                  "cpu_max": [uint(v) for v in cpu], "cpu_stat": cpu_stat, "pids_events": pid_events,
                  "members": sorted(members), "initial": initial, "network": network}
        if any(before[k] != 0 or after[k] != 0 for k in OOM) or peak > MEMORY:
            raise Adverse("oom-or-peak-limit", result)
        return result


class ProcWitness:
    def __init__(self, pid, group, domain, expected_exe=None, sha=None):
        need(type(pid) is int and 0 < pid < (1 << 31), "process-id")
        self.pid, self.prefix, self.fds = pid, "/proc/" + str(pid), {}
        self.group, self.domain, self.expected_sha = group, domain, sha
        self.closed = False
        try:
            self.fds["proc"] = os.open(self.prefix, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            self.fds["exe"] = os.open("exe", os.O_RDONLY | os.O_CLOEXEC, dir_fd=self.fds["proc"])
            for kind in ("time", "time_for_children"):
                self.fds[kind] = os.open("ns/" + kind, os.O_RDONLY | os.O_CLOEXEC, dir_fd=self.fds["proc"])
            self.ids = {k: identity(fd) for k, fd in self.fds.items()}
            self.exe_stamp = stamp(os.fstat(self.fds["exe"]))
            if expected_exe is not None:
                need(self.exe_stamp[:2] == expected_exe[:2], "process-executable-not-input")
            self.process = clockprobe.proc_stat(clockprobe.read_file(self.prefix + "/stat", 8192))
            self.status = clockprobe.proc_status(clockprobe.read_file(self.prefix + "/status", 16384))
            need(self.process["pid"] == pid and self.status["Pid"] == [pid] and
                 self.status["Uid"] == [501] * 4 and self.status["Gid"] == [20] * 4,
                 "process-birth-or-credentials")
            if sha is not None:
                need(file_hash(self.fds["exe"], 256 * 1024 * 1024) == sha, "process-executable-sha")
            self.read()
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, _type, error, _trace):
        try:
            self.close()
        except OSError:
            if error is None:
                raise
            error.cleanup_failed = True

    def close(self):
        if not self.closed:
            self.closed = True
            close_fds(list(self.fds.values()))

    def held(self):
        need(all(identity(fd) == self.ids[key] for key, fd in self.fds.items()) and
             stamp(os.fstat(self.fds["exe"])) == self.exe_stamp and
             self.ids["time"] == self.ids["time_for_children"] == self.domain,
             "process-held-binding-changed")

    def read(self, pending_exit=False):
        self.held()
        try:
            process = clockprobe.proc_stat(clockprobe.read_file(self.prefix + "/stat", 8192))
            status = clockprobe.proc_status(clockprobe.read_file(self.prefix + "/status", 16384))
            need(process == self.process and status == self.status and
                 clockprobe.read_file(self.prefix + "/cgroup", 4096) == self.group,
                 "process-birth-status-or-cgroup-changed")
            for key in ("proc", "exe", "time", "time_for_children"):
                path = self.prefix + {"proc": "", "exe": "/exe", "time": "/ns/time",
                                      "time_for_children": "/ns/time_for_children"}[key]
                fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
                try:
                    need(identity(fd) == self.ids[key], "process-named-binding-changed")
                finally:
                    os.close(fd)
        except OSError as error:
            if pending_exit and error.errno in (errno.ENOENT, errno.ESRCH):
                return {"pid": self.pid, "process": self.process, "available": False,
                        "reason": "announced-exit-pending-owned-reap", "rss": None, "hwm": None, "pss": None}
            raise
        return {"pid": self.pid, "process": process, "status": status, "available": True,
                "namespace": self.domain, "executable_identity": self.ids["exe"],
                "rss": None, "hwm": None, "pss": None,
                "attribution_reason": "optional-process-memory-not-collected"}

    def final_hash(self):
        self.held()
        if self.expected_sha is not None:
            need(file_hash(self.fds["exe"], 256 * 1024 * 1024) == self.expected_sha,
                 "final-held-executable-sha")
        return {"pid": self.pid, "process": self.process, "exe_sha256": self.expected_sha,
                "namespace": self.domain, "named_proc_after_reap": "NOT_REQUIRED"}


def read_line(stream, maximum=4096):
    raw = stream.readline(maximum + 1)
    need(raw.endswith(b"\n") and len(raw) <= maximum, "control-line-framing")
    return raw


def control(raw, nonce):
    value = strict_json(raw)
    need(type(value) is dict and value.get("nonce") == nonce and type(value.get("kind")) is str,
         "control-binding")
    return value


def sampler(nonce, cli_sha, keeper_pid):
    need(os.getppid() == keeper_pid and keeper_pid == 1 and os.getuid() == 501 and os.getgid() == 20,
         "sampler-parent")
    begin_clock = qualified_clock("observer", "sg32-sampler-before", nonce)
    domain = begin_clock["contexts"]["thread_self"]["time"]
    with contextlib.ExitStack() as stack:
        group = stack.enter_context(Cgroup())
        own = stack.enter_context(ProcWitness(os.getpid(), group.raw, domain))
        keeper = stack.enter_context(ProcWitness(keeper_pid, group.raw, domain))
        initial = group.snapshot(initial=True)
        need(set(initial["members"]) == {keeper_pid, os.getpid()}, "sampler-initial-membership")
        last = initial["read_bracket_ns"][1]
        ordinal = 0
        emit({"kind": "SAMPLER_READY", "nonce": nonce, "ordinal": ordinal,
              "facts": initial, "clock": begin_clock, "processes": [keeper.read(), own.read()]})
        selector = stack.enter_context(selectors.DefaultSelector())
        selector.register(sys.stdin.buffer, selectors.EVENT_READ)
        need(bool(selector.select(2)), "node-bind-gap")
        first = control(read_line(sys.stdin.buffer), nonce)
        need(set(first) == {"kind", "nonce", "pid", "start_ticks"} and first["kind"] == "NODE" and
             type(first["start_ticks"]) is int, "node-control")
        node_input = os.stat("/inputs/corsad", follow_symlinks=False)
        node = stack.enter_context(ProcWitness(first["pid"], group.raw, domain, stamp(node_input), cli_sha))
        need(node.process["start_ticks"] == first["start_ticks"] and node.process["ppid"] == keeper_pid,
             "node-owned-birth")
        pending, reaped, due = False, None, last + CADENCE_NS
        ends, reap_edge = [last], None
        while reaped is None:
            now = clockprobe.boottime()
            wait = max(0, (due - now) / 1000000000)
            messages = selector.select(min(wait, .5))
            for _key, _mask in messages:
                message = control(read_line(sys.stdin.buffer), nonce)
                if message["kind"] == "STOPPING":
                    need(set(message) == {"kind", "nonce"} and not pending, "stopping-order")
                    node.read()
                    pending = True
                    emit({"kind": "STOPPING_ACK", "nonce": nonce, "pid": node.pid,
                          "start_ticks": node.process["start_ticks"], "at_ns": clockprobe.boottime()})
                elif message["kind"] == "REAP":
                    need(set(message) == {"kind", "nonce", "pid", "start_ticks", "exit", "at_ns"} and
                         pending and message["pid"] == node.pid and type(message["pid"]) is int and
                         message["start_ticks"] == node.process["start_ticks"] and
                         type(message["start_ticks"]) is int and type(message["exit"]) is int and
                         message["exit"] == 0 and type(message["at_ns"]) is int and
                         0 <= message["at_ns"] <= clockprobe.boottime(), "owned-reap-edge")
                    preceding = [end for end in ends if end <= message["at_ns"]]
                    need(bool(preceding) and message["at_ns"] - max(preceding) <= GAP_NS, "owned-reap-sample-gap")
                    reap_edge, reaped = max(preceding), message
                else:
                    raise Fault("unexpected-sampler-control")
            if reaped is not None or clockprobe.boottime() >= due:
                facts = group.snapshot()
                need(facts["read_bracket_ns"][0] >= last and facts["read_bracket_ns"][1] - last <= GAP_NS,
                     "sample-gap")
                allowed = {keeper_pid, os.getpid()} | (set() if reaped else {node.pid})
                need(set(facts["members"]) <= allowed and {keeper_pid, os.getpid()} <= set(facts["members"]),
                     "sample-membership")
                if not pending:
                    need(node.pid in facts["members"], "unannounced-node-loss")
                processes = [keeper.read(), own.read()]
                if reaped is None:
                    processes.append(node.read(pending))
                last = facts["read_bracket_ns"][1]
                ends = (ends + [last])[-5:]
                ordinal += 1
                emit({"kind": "SAMPLE" if reaped is None else "SAMPLER_FINAL", "nonce": nonce,
                      "ordinal": ordinal, "requested_at_ns": due, "facts": facts, "processes": processes,
                      "node_reap": reaped, "node_reap_edge_end_ns": reap_edge,
                      "node_final": node.final_hash() if reaped else None})
                if ordinal == 1:
                    emit({"kind": "NODE_BOUND", "nonce": nonce, "pid": node.pid,
                          "start_ticks": node.process["start_ticks"]})
                due = last + CADENCE_NS
        emit({"kind": "SAMPLER_SEALED", "nonce": nonce,
              "clock": qualified_clock("observer", "sg32-sampler-after", nonce)})
    return 0


class OwnedChild:
    def __init__(self, argv, owned, env, interactive=False):
        self.buffers, self.raw_bytes, self.eof = {}, 0, set()
        self.p = subprocess.Popen(argv, stdin=subprocess.PIPE if interactive else subprocess.DEVNULL,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
                                  close_fds=True, start_new_session=False)
        owned.append(self)
        for name, stream in (("out", self.p.stdout), ("err", self.p.stderr)):
            self.buffers[name] = bytearray()
            os.set_blocking(stream.fileno(), False)

    def send(self, value):
        raw = encode(value, 4096)
        need(self.p.stdin.write(raw) == len(raw), "child-control-short-write")
        self.p.stdin.flush()

    def terminate(self, deadline):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout=max(.001, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait(timeout=max(.001, deadline - time.monotonic()))
        for stream in (self.p.stdin, self.p.stdout, self.p.stderr):
            if stream is not None:
                stream.close()


def log_chunk(child, stream, raw, secret, nonce, eof=False):
    buffer = child.buffers[stream]
    buffer.extend(raw)
    forbidden = [v.encode("ascii") for v in secret.values()]
    need(not any(value in buffer for value in forbidden), "secret-in-node-output")
    retain = 0 if eof else max(len(value) for value in forbidden) - 1
    count = max(0, len(buffer) - retain)
    while count:
        size = min(count, 16384)
        body = bytes(buffer[:size])
        del buffer[:size]
        count -= size
        emit({"kind": "LOG", "nonce": nonce, "stream": stream, "bytes": len(body),
              "raw_base64": base64.b64encode(body).decode("ascii"),
              "sha256": hashlib.sha256(body).hexdigest()})


def daemon_environment(secret):
    return {"PATH": "/usr/bin:/bin", "HOME": "/work/home", "TMPDIR": "/work/tmp", "LC_ALL": "C",
            "GOMAXPROCS": "1", "CORSA_DATA_DIR": "/work/data", "CORSA_NODE_TYPE": "full",
            "CORSA_LISTENER": "1", "CORSA_LISTEN_ADDRESS": "127.0.0.1:64646",
            "CORSA_BOOTSTRAP_PEERS": "127.0.0.1:64647", "CORSA_RPC_HOST": "127.0.0.1",
            "CORSA_RPC_PORT": "46464", "CORSA_RPC_USERNAME": secret["username"],
            "CORSA_RPC_PASSWORD": secret["password"]}


def read_secret():
    fd = os.open("/run/c1/auth.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        original = os.fstat(fd)
        need(stat.S_ISREG(original.st_mode) and stat.S_IMODE(original.st_mode) == 0o600 and
             original.st_uid == 501 and original.st_gid == 20 and original.st_nlink == 1 and
             0 < original.st_size <= 1024 and os.fstatvfs(fd).f_flag & os.ST_RDONLY, "secret-custody")
        value = strict_json(read_fd(fd, 1024))
        need(type(value) is dict and set(value) == {"username", "password"} and
             all(type(v) is str and re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", v) for v in value.values()) and
             stamp(os.fstat(fd)) == stamp(original) == stamp(os.lstat("/run/c1/auth.json")), "secret-format")
        return value
    finally:
        os.close(fd)


def node_status(secret):
    auth = base64.b64encode((secret["username"] + ":" + secret["password"]).encode()).decode()
    connection = http.client.HTTPConnection("127.0.0.1", 46464, timeout=.25)
    try:
        connection.request("POST", "/rpc/v1/exec", body=b'{"command":"getNodeStatus","args":{}}',
                           headers={"Content-Type": "application/json", "Authorization": "Basic " + auth})
        reply = connection.getresponse()
        need(reply.status == 200, "readiness-rpc-status")
        raw = reply.read(1048577)
        need(len(raw) <= 1048576, "readiness-rpc-limit")
        value = strict_json(raw)
        need(type(value) is dict and type(value.get("identity")) is str and
             re.fullmatch(r"[0-9a-f]{40}", value["identity"]) and
             value.get("identity") != "0" * 40 and value.get("address") == value["identity"] and
             type(value.get("started_at")) is str and value["started_at"].endswith("Z"), "readiness-rpc-binding")
        datetime.datetime.fromisoformat(value["started_at"].replace("Z", "+00:00"))
        return value["identity"], value["started_at"]
    finally:
        connection.close()


def state_walk(poll=None):
    begin, total, entries, opened = clockprobe.boottime(), 0, 1, []
    root = os.open("/work", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    opened.append(root)
    pending = [root]
    try:
        while pending:
            if poll is not None:
                poll()
            directory = pending.pop()
            with os.scandir(directory) as iterator:
                for entry in iterator:
                    if poll is not None:
                        poll()
                    entries += 1
                    need(entries <= 4096, "state-entry-limit")
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        fd = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                     dir_fd=directory)
                        opened.append(fd)
                        need(stamp(os.fstat(fd)) == stamp(info), "state-directory-changed")
                        pending.append(fd)
                    else:
                        need(stat.S_ISREG(info.st_mode) and info.st_size >= 0, "state-special-or-symlink")
                        total += info.st_size
                        need(total <= 64 * 1024 * 1024, "state-byte-limit")
        end = clockprobe.boottime()
        need(begin <= end, "state-clock-regression")
        return {"bytes": total, "entries": entries, "bracket_ns": [begin, end],
                "scope": "bounded-nonatomic-logical-state-walk"}
    finally:
        close_fds(opened)


def node_exit_fact(child, birth, phase, stop_requested=False):
    code = child.p.poll()
    if code is None:
        return None
    need(type(code) is int, "node-returncode-type")
    return {"kind": "NODE_EXIT", "pid": child.p.pid, "process": birth,
            "birth_available": birth is not None, "returncode": code, "phase": phase,
            "stop_requested": stop_requested, "early": not stop_requested, "owned_reaped": True}


def node_running(child, birth, phase):
    fact = node_exit_fact(child, birth, phase)
    if fact is not None:
        raise Adverse("node-early-exit", fact)


def keeper(nonce, cli_sha):
    need(os.getpid() == 1 and os.getuid() == 501 and os.getgid() == 20, "keeper-credentials")
    clock = qualified_clock("keeper", "sg32-keeper-before", nonce)
    domain = clock["contexts"]["thread_self"]["time"]
    secret, owned, messages = read_secret(), [], []
    host_buffer, host_messages, checked = bytearray(), [], []
    bootstrap = "import sys;sys.path.insert(0,'/inputs');import sg32_service;raise SystemExit(sg32_service.main())"
    start, normal = time.monotonic(), False
    node_child, birth, stop_requested = None, None, False
    with contextlib.ExitStack() as stack:
        group = stack.enter_context(Cgroup())
        own = stack.enter_context(ProcWitness(1, group.raw, domain))
        initial = group.snapshot(initial=True)
        need(initial["members"] == [1], "keeper-initial-membership")
        selector = stack.enter_context(selectors.DefaultSelector())
        try:
            sampler_child = OwnedChild(["/usr/bin/python3", "-I", "-S", "-B", "-c", bootstrap,
                                        "sampler", nonce, cli_sha, "1"], owned,
                                       {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}, True)
            def register(child, label):
                for name, stream in (("out", child.p.stdout), ("err", child.p.stderr)):
                    selector.register(stream, selectors.EVENT_READ, (child, label, name))
            register(sampler_child, "sampler")
            def pump(deadline, wanted=None, once=False):
                while once or time.monotonic() < deadline:
                    need(time.monotonic() < deadline, "service-phase-deadline")
                    if wanted is not None:
                        for i, packet in enumerate(messages):
                            if packet.get("kind") == wanted:
                                return messages.pop(i)
                    events = selector.select(0 if once else min(.05, max(0, deadline - time.monotonic())))
                    for key, _mask in events:
                        child, label, stream = key.data
                        if label == "host":
                            chunk = os.read(key.fd, 4096)
                            need(bool(chunk), "host-control-eof")
                            host_buffer.extend(chunk)
                            need(len(host_buffer) <= 8192, "host-control-limit")
                            while b"\n" in host_buffer:
                                line, _, rest = host_buffer.partition(b"\n")
                                host_buffer[:] = rest
                                need(len(line) <= 4096 and len(host_messages) < 5, "host-control-line-limit")
                                host_messages.append(control(line, nonce))
                            continue
                        raw = os.read(key.fd, 8192)
                        if not raw:
                            selector.unregister(key.fileobj)
                            child.eof.add(stream)
                            if label == "node":
                                log_chunk(child, stream, b"", secret, nonce, True)
                            else:
                                need(not child.buffers[stream], "sampler-partial-eof")
                            continue
                        child.raw_bytes += len(raw)
                        need(child.raw_bytes <= RAW_LIMIT, "child-output-limit")
                        if label == "sampler":
                            need(stream == "out", "sampler-stderr")
                            child.buffers[stream].extend(raw)
                            need(len(child.buffers[stream]) <= 65536, "sampler-line-limit")
                            while b"\n" in child.buffers[stream]:
                                line, _, rest = child.buffers[stream].partition(b"\n")
                                child.buffers[stream] = bytearray(rest)
                                packet = control(line, nonce)
                                emit(packet)
                                if packet["kind"] == "ERROR":
                                    if packet.get("status") == "FAIL":
                                        raise Adverse("sampler-adverse", packet.get("facts"))
                                    raise Fault("sampler-refused")
                                if packet["kind"] in ("SAMPLER_READY", "NODE_BOUND", "STOPPING_ACK", "SAMPLER_FINAL", "SAMPLER_SEALED"):
                                    need(len(messages) < 8, "sampler-control-limit")
                                    messages.append(packet)
                        else:
                            log_chunk(child, stream, raw, secret, nonce)
                    if wanted is None:
                        return None
                    for i, packet in enumerate(messages):
                        if packet.get("kind") == wanted:
                            return messages.pop(i)
                    need(sampler_child.p.poll() is None, "sampler-exited-before-control")
                raise Fault("service-phase-deadline")
            ready = pump(min(start + 20, time.monotonic() + 2), "SAMPLER_READY")
            need(ready.get("nonce") == nonce and ready.get("facts", {}).get("cgroup_identity") == initial["cgroup_identity"]
                 and set(ready["facts"].get("members", [])) == {1, sampler_child.p.pid}, "sampler-ready-binding")
            node_child = OwnedChild(["/inputs/corsad"], owned, daemon_environment(secret))
            register(node_child, "node")
            node_running(node_child, None, "before-birth-read")
            try:
                birth = clockprobe.proc_stat(clockprobe.read_file("/proc/" + str(node_child.p.pid) + "/stat", 8192))
            except OSError:
                node_running(node_child, None, "birth-read-unavailable")
                raise
            sampler_child.send({"kind": "NODE", "nonce": nonce, "pid": node_child.p.pid,
                                "start_ticks": birth["start_ticks"]})
            bound = pump(min(start + 20, time.monotonic() + 2), "NODE_BOUND")
            need(bound.get("pid") == node_child.p.pid and bound.get("start_ticks") == birth["start_ticks"],
                 "node-bound-binding")
            while True:
                need(time.monotonic() < start + 20, "startup-deadline")
                node_running(node_child, birth, "startup")
                try:
                    server, started_at = node_status(secret)
                    break
                except (OSError, http.client.HTTPException, Fault):
                    pump(min(start + 20, time.monotonic() + .05))
            startup_state = state_walk(lambda: pump(start + 20, once=True))
            emit({"kind": "READY", "nonce": nonce, "identity": server, "started_at": started_at,
                  "clock": clock, "initial": initial, "state": startup_state})
            selector.register(sys.stdin.buffer, selectors.EVENT_READ, (None, "host", "in"))
            # Closed phase controls never stand in for the host's driver oracle.
            stopped = False
            while not stopped:
                need(time.monotonic() < start + 150 and sampler_child.p.poll() is None, "work-child-or-deadline")
                node_running(node_child, birth, "work")
                pump(min(start + 150, time.monotonic() + .05))
                while host_messages:
                    message = host_messages.pop(0)
                    if message["kind"] == "CHECK":
                        need(set(message) == {"kind", "nonce", "phase"} and len(checked) < 4 and
                             message["phase"] == PHASES[len(checked)], "host-phase-order")
                        facts = state_walk(lambda: pump(start + 150, once=True))
                        checked.append(message["phase"])
                        emit({"kind": "STATE_CHECK", "nonce": nonce, "phase": message["phase"], **facts})
                    else:
                        need(message["kind"] == "STOP" and set(message) == {"kind", "nonce"} and
                             checked == list(PHASES) and not host_buffer and not host_messages,
                             "host-stop-contract")
                        stopped = True
                        break
            selector.unregister(sys.stdin.buffer)
            stop_deadline = min(start + 165, time.monotonic() + 15)
            sampler_child.send({"kind": "STOPPING", "nonce": nonce})
            ack = pump(min(stop_deadline, time.monotonic() + 2), "STOPPING_ACK")
            need(ack.get("pid") == node_child.p.pid and ack.get("start_ticks") == birth["start_ticks"],
                 "stopping-ack-binding")
            node_running(node_child, birth, "after-stopping-ack-before-signal")
            stop_requested = True
            node_child.p.send_signal(signal.SIGTERM)
            while node_child.p.poll() is None or node_child.eof != {"out", "err"}:
                need(time.monotonic() < stop_deadline, "node-stop-deadline")
                pump(stop_deadline)
            code = node_child.p.wait(timeout=.001)
            if code != 0:
                raise Adverse("node-nonnormal-exit", node_exit_fact(node_child, birth, "normal-stop", True))
            reaped_at = clockprobe.boottime()
            sampler_child.send({"kind": "REAP", "nonce": nonce, "pid": node_child.p.pid,
                                "start_ticks": birth["start_ticks"], "exit": 0, "at_ns": reaped_at})
            join_deadline = min(start + 170, time.monotonic() + 5)
            final = pump(join_deadline, "SAMPLER_FINAL")
            sealed = pump(join_deadline, "SAMPLER_SEALED")
            while sampler_child.p.poll() is None or sampler_child.eof != {"out", "err"}:
                need(time.monotonic() < join_deadline, "sampler-join-deadline")
                pump(join_deadline)
            need(sampler_child.p.wait(timeout=.001) == 0 and not sampler_child.buffers["out"], "sampler-nonnormal-exit")
            final_state = state_walk(lambda: pump(start + 180, once=True))
            final_group = group.snapshot()
            need(final_group["members"] == [1], "final-keeper-only-membership")
            emit({"kind": "FINAL", "nonce": nonce, "facts": final_group, "node_exit": 0,
                  "sampler_exit": 0, "sampler_final": final, "sampler_sealed": sealed,
                  "clock": qualified_clock("keeper", "sg32-keeper-after", nonce),
                  "state": final_state, "checked_phases": checked,
                  "keeper_process": own.read(), "keeper_exit_tail": "UNKNOWN"})
            normal = True
        finally:
            # Failure cleanup is never a successful drain. Container removal remains host-owned.
            active_error, cleanup_error, child_exits = sys.exc_info()[1], None, []
            if active_error is not None and node_child is not None:
                observed = node_exit_fact(node_child, birth, "before-failure-cleanup", stop_requested)
                if observed is not None:
                    active_error.observed_node_exit = observed
            for child in reversed(owned):
                try:
                    child.terminate(time.monotonic() + (1 if not normal else .001))
                except (OSError, subprocess.SubprocessError) as error:
                    cleanup_error = error
                child_exits.append({"pid": child.p.pid, "role": "node" if child is node_child else "sampler",
                                    "process": birth if child is node_child else None,
                                    "returncode": child.p.returncode, "scope": "failure-cleanup" if not normal else "normal-close"})
            if active_error is not None:
                active_error.child_exits = child_exits
            if cleanup_error is not None:
                if active_error is None:
                    raise cleanup_error
                active_error.cleanup_failed = True
    return 0


def manifest(value, nonce):
    need(type(value) is dict and set(value) == {"nonce", "files"} and value["nonce"] == nonce and
         type(value["files"]) is dict and set(value["files"]) == set(INPUTS), "input-manifest-fields")
    for name, entry in value["files"].items():
        need(type(entry) is dict and set(entry) == {"sha256", "bytes"} and type(entry["sha256"]) is str and
             re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) and type(entry["bytes"]) is int and
             0 < entry["bytes"] <= (256 * 1024 * 1024 if name in ("corsad", "c2driver") else 65536),
             "input-manifest-entry")
    return value


PREPARER_PATHS = {"/inputs": "inputs", "/run/c1": "secret-dir", "/work": "work",
                  "/inputs/manifest.json": "manifest", "/run/c1/auth.json": "auth",
                  **{"/inputs/" + name: name for name in INPUTS},
                  **{"/work/" + name: name for name in ("data", "home", "tmp")},
                  None: None}
PREPARER_OPS = {"stdin-read", "lstat", "listdir", "manifest-read", "open", "fstat", "hash",
                "fchown", "fchmod", "close", "chown", "chmod", "mkdir", "write", "fsync", "emit"}


def preparer_call(operation, path, function, *args):
    """Capture only fixed-label, nonsecret metadata before the original operation."""
    need(operation in PREPARER_OPS and path in PREPARER_PATHS, "preparer-diagnostic-label")
    facts = {"scope": "PREPARER_OPERATION", "operation": operation, "path_label": PREPARER_PATHS[path],
             "errno": None, "actor": None, "metadata": None, "metadata_errno": None,
             "metadata_unavailable": True}
    try:
        ids = (os.getuid(), os.getgid(), os.geteuid(), os.getegid())
        if all(type(value) is int and 0 <= value < (1 << 32) for value in ids):
            facts["actor"] = dict(zip(("uid", "gid", "euid", "egid"), ids))
        if path is not None:
            info = os.lstat(path)
            values = (info.st_uid, info.st_gid, info.st_mode, info.st_nlink)
            if all(type(value) is int and 0 <= value < (1 << 64) for value in values):
                facts["metadata"] = dict(zip(("uid", "gid", "mode", "nlink"), values))
                facts["metadata_unavailable"] = False
    except Exception as diagnostic_error:
        value = getattr(diagnostic_error, "errno", None)
        facts["metadata_errno"] = value if type(value) is int and 0 <= value <= 4095 else None
    try:
        return function(*args)
    except OSError as error:
        value = error.errno
        facts["errno"] = value if type(value) is int and 0 <= value <= 4095 else None
        try:
            error.preparer_diagnostic = facts
        except Exception:
            pass
        raise


def preparer(nonce):
    need(os.getuid() == 0 and os.getgid() == 0, "preparer-credentials")
    start = time.monotonic()
    config = manifest(strict_json(preparer_call("stdin-read", None, read_line, sys.stdin.buffer, 8192)), nonce)
    need(all(stat.S_ISDIR(preparer_call("lstat", path, os.lstat, path).st_mode) for path in ("/inputs", "/run/c1", "/work")),
         "volume-root-not-directory")
    need(set(preparer_call("listdir", "/inputs", os.listdir, "/inputs")) == set(INPUTS) | {"manifest.json"} and
         preparer_call("listdir", "/run/c1", os.listdir, "/run/c1") == [] and preparer_call("listdir", "/work", os.listdir, "/work") == [], "volumes-not-fresh")
    need(manifest(strict_json(preparer_call("manifest-read", "/inputs/manifest.json", clockprobe.read_file, "/inputs/manifest.json", 8192)), nonce) == config,
         "uploaded-manifest-mismatch")
    for name in INPUTS + ("manifest.json",):
        path = "/inputs/" + name
        fd = preparer_call("open", path, os.open, path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info = preparer_call("fstat", path, os.fstat, fd)
            need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "input-file-shape")
            if name in INPUTS:
                entry = config["files"][name]
                need(info.st_size == entry["bytes"] and preparer_call("hash", path, file_hash, fd, entry["bytes"]) == entry["sha256"],
                     "input-file-binding")
            preparer_call("fchown", path, os.fchown, fd, 0, 0)
            preparer_call("fchmod", path, os.fchmod, fd, 0o555 if name in ("corsad", "c2driver") else 0o444)
            need(time.monotonic() - start <= 20, "preparer-deadline")
        finally:
            preparer_call("close", path, os.close, fd)
    preparer_call("chown", "/inputs", os.chown, "/inputs", 0, 0)
    preparer_call("chmod", "/inputs", os.chmod, "/inputs", 0o555)
    preparer_call("chown", "/work", os.chown, "/work", 0, 0)
    preparer_call("chmod", "/work", os.chmod, "/work", 0o755)
    for name in ("data", "home", "tmp"):
        path = "/work/" + name
        preparer_call("mkdir", path, os.mkdir, path, 0o700)
        preparer_call("chmod", path, os.chmod, path, 0o700)
        preparer_call("chown", path, os.chown, path, 501, 20)
    secret = encode({"username": "sg32_" + nonce, "password": secrets.token_hex(32)}, 1024)
    path = "/run/c1/auth.json"
    fd = preparer_call("open", path, os.open, path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        need(preparer_call("write", path, os.write, fd, secret) == len(secret), "secret-short-write")
        preparer_call("fchmod", path, os.fchmod, fd, 0o600)
        preparer_call("fchown", path, os.fchown, fd, 501, 20)
        preparer_call("fsync", path, os.fsync, fd)
        created = preparer_call("fstat", path, os.fstat, fd)
        need(stat.S_IMODE(created.st_mode) == 0o600 and created.st_uid == 501 and
             created.st_gid == 20 and created.st_nlink == 1, "prepared-secret-metadata")
    finally:
        preparer_call("close", path, os.close, fd)
    preparer_call("chmod", "/run/c1", os.chmod, "/run/c1", 0o700)
    for path in ("/inputs", "/run/c1", "/work"):
        fd = preparer_call("open", path, os.open, path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if path == "/run/c1":
                preparer_call("fchown", path, os.fchown, fd, 501, 20)
            preparer_call("fsync", path, os.fsync, fd)
        finally:
            preparer_call("close", path, os.close, fd)
    preparer_call("emit", None, emit, {"kind": "PREPARED", "nonce": nonce, "files": config["files"],
          "credential": {"mode": 384, "uid": 501, "gid": 20, "nlink": 1},
          "state_children": ["data", "home", "tmp"]})
    return 0


def dispatch(argv=None):
    args = sys.argv[1:] if argv is None else argv
    need(type(args) is list and len(args) >= 2 and args[0] in ("preparer", "keeper", "sampler") and
         type(args[1]) is str and re.fullmatch(r"[0-9a-f]{32}", args[1]), "role-arguments")
    role, nonce = args[:2]
    if role == "preparer":
        need(len(args) == 2, "preparer-arguments")
        return preparer(nonce)
    need(len(args) == (3 if role == "keeper" else 4) and type(args[2]) is str and re.fullmatch(r"[0-9a-f]{64}", args[2]),
         "runtime-arguments")
    if role == "sampler":
        need(args[3] == "1", "sampler-parent-argument")
    return keeper(nonce, args[2]) if role == "keeper" else sampler(nonce, args[2], int(args[3]))


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    try:
        return dispatch(args)
    except (Fault, clockprobe.Fault, OSError, ValueError, http.client.HTTPException, subprocess.SubprocessError) as error:
        nonce = args[1] if len(args) > 1 and type(args[1]) is str and re.fullmatch(r"[0-9a-f]{32}", args[1]) else None
        observed = getattr(error, "observed_node_exit", None)
        failed = isinstance(error, Adverse) or (observed is not None and
                                               (observed["returncode"] != 0 or observed["early"]))
        emit({"kind": "ERROR", "nonce": nonce, "status": "FAIL" if failed else "UNKNOWN",
              "reason": str(error) if isinstance(error, (Fault, clockprobe.Fault)) else type(error).__name__,
              "facts": error.facts if isinstance(error, Adverse) else getattr(error, "preparer_diagnostic", None),
              "cleanup_failed": getattr(error, "cleanup_failed", False),
              "observed_node_exit": observed, "child_exits": getattr(error, "child_exits", [])})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
