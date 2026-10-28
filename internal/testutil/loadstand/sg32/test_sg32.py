"""Finite SG32 actual-function controls; no child, Docker, app or Go execution."""
import base64
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import struct
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
NONCE = "a" * 32
CLI = "b" * 64
SERVER = "f" * 40


def load_file(name):
    path = HERE / (name + ".py")
    if name == "clockprobe" and not path.exists():
        path = HERE.parent / "clockprobe" / "probe.py"
    module = types.ModuleType("sg32_test_subject_" + name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    return module


def subjects():
    common, clock = load_file("sg32_common"), load_file("clockprobe")
    with mock.patch.dict(sys.modules, {"sg32_common": common, "clockprobe": clock}):
        return common, clock, load_file("sg32_service"), load_file("sg32_host")


def proc_stat(pid=42, birth=111, ppid=1):
    fields = ["0"] * 20
    fields[0], fields[1], fields[19] = "S", str(ppid), str(birth)
    return (str(pid) + " (owned (actor)) " + " ".join(fields) + "\n").encode()


def proc_status(pid=42):
    return ("Pid:\t%d\nTgid:\t%d\nNSpid:\t%d\nUid:\t501 501 501 501\nGid:\t20 20 20 20\n" % (pid, pid, pid)).encode()


def driver_rows():
    rows = []
    phases = ("thirty-two-proven", "sixteen-survivors", "thirty-two-recovered", "zero-drained")
    work = (32, 176, 224, 224)
    for i, numbers in enumerate((range(1, 33), range(17, 33), range(17, 49), [])):
        peers = [{"ordinal": n, "identity": "%040x" % n, "server": SERVER,
                  "local": "127.0.0.1:" + str(40000 + n), "remote": "127.0.0.1:64646",
                  "useful_exchanges": 1 if i == 0 else 6 if i == 1 else 7 if n <= 32 else 2}
                 for n in numbers]
        rows.append({"kind": "c2b", "nonce": NONCE, "phase": phases[i],
                     "verified_channels": len(peers), "peers": peers if i != 3 else None,
                     "cached_live": {"ready": False}, "cached_live_acceptance": False,
                     "application_charge": {"frames": 6 * work[i], "bytes": 1000 * work[i]}})
    return rows


def network_raws():
    header = b" sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
    return {"fib_trie": b"Main:\n |-- 127.0.0.1\n /32 host LOCAL\n",
            "if_inet6": b"00000000000000000000000000000001 01 80 10 80 lo\n",
            "route": b"Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n",
            "ipv6_route": b"00000000000000000000000000000001 80 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00000001 lo\n",
            "tcp": header, "tcp6": header, "udp": header, "udp6": header}


def container_meta(role, volumes, service_id="1" * 64):
    cpu, memory, pids = {"preparer": (250000000, 134217728, 32),
                         "service": (1000000000, 512000000, 64),
                         "driver": (1000000000, 268435456, 32)}[role]
    labels = {"corsa.sg32.owner": NONCE, "corsa.sg32.role": role}
    mounts = [{"Type": "volume", "Destination": "/inputs", "Name": volumes["inputs"], "RW": role == "preparer"},
              {"Type": "volume", "Destination": "/run/c1", "Name": volumes["secret"], "RW": role == "preparer"}]
    if role != "driver": mounts.append({"Type": "volume", "Destination": "/work", "Name": volumes["state"], "RW": True})
    return {"Image": "sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e",
            "Name": "/corsa-d1-sg32-" + NONCE + "-" + role,
            "Config": {"Labels": labels, "User": "0:0" if role == "preparer" else "501:20",
                       "Env": ["CORSA_C2B32_RUN=1"] if role == "driver" else []},
            "HostConfig": {"Privileged": False, "ReadonlyRootfs": True, "CapDrop": ["ALL"],
                           "CapAdd": ["CHOWN"] if role == "preparer" else [],
                           "SecurityOpt": ["no-new-privileges"], "RestartPolicy": {"Name": "no"},
                           "Memory": memory, "MemorySwap": memory, "NanoCpus": cpu, "PidsLimit": pids,
                           "NetworkMode": "container:" + service_id if role == "driver" else "none",
                           "PidMode": "", "CgroupnsMode": "private", "Binds": [], "PortBindings": {}},
            "Mounts": mounts, "State": {"Running": False}}


def inert_elf():
    ident = b"\x7fELF\x02\x01\x01" + b"\0" * 9
    return struct.pack("<16sHHIQQQIHHHHHH", ident, 2, 183, 1, 0, 64, 0, 0, 64, 56, 1, 0, 0, 0) + struct.pack("<IIQQQQQQ", 1, 5, 120, 0, 0, 0, 0, 4096)


def process_record(pid):
    return {"pid": pid, "process": {"pid": pid, "ppid": 0 if pid == 1 else 1, "start_ticks": 111},
            "status": {"Pid": [pid], "Tgid": [pid], "NSpid": [pid], "Uid": [501] * 4, "Gid": [20] * 4},
            "available": True, "namespace": [5, 901], "executable_identity": [5, 1000 + pid],
            "rss": None, "hwm": None, "pss": None}


def clock_record(pid, at, phase="fixture"):
    contexts = {}
    offsets = b"monotonic 0 0\nboottime 0 0\n"
    for name, target in (("self", pid), ("thread_self", pid), ("numeric_pid", pid)):
        pr = process_record(target)
        row = {"process": pr["process"], "status": pr["status"], "time": [5, 901], "time_for_children": [5, 901],
               "offsets_for_children": {"raw_base64": base64.b64encode(offsets).decode(),
                                        "sha256": hashlib.sha256(offsets).hexdigest(),
                                        "values": {"monotonic": {"seconds": 0, "nanoseconds": 0}, "boottime": {"seconds": 0, "nanoseconds": 0}}}}
        contexts[name] = row
    thread = contexts["thread_self"]
    witness = {k: copy.deepcopy(thread[k]) for k in ("process", "status", "time", "time_for_children")}
    thread["offsets_reader"] = {"path": "/proc/" + str(pid), "thread_self_link_before": str(pid) + "/task/" + str(pid),
                                "thread_self_link_after": str(pid) + "/task/" + str(pid),
                                "before": copy.deepcopy(witness), "after": copy.deepcopy(witness)}
    return {"schema": "d1-clock-probe-v1", "nonce": NONCE, "phase": phase, "role": "keeper" if pid == 1 else "observer", "pid": pid, "native_tid": pid,
            "complete": True, "supported_source_guard": True, "shared_time_domain": True, "errors": [],
            "contexts": contexts, "own_boottime_bracket_ns": [at, at + 1], "own_boottime_observation_ns": at}


def metric_record(service, at, members, number=0, initial=False):
    events = {name: 0 for name in ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")}
    return {"cgroup_identity": [5, 801], "group_path": "/", "read_bracket_ns": [at, at + 10],
            "peak_bracket_ns": [at + 2, at + 3], "current_bracket_ns": [at + 4, at + 5],
            "peak": 2000 + number, "current": 1000 + number,
            "events_before": copy.deepcopy(events), "events_after": copy.deepcopy(events),
            "limits": {"memory.max": 512000000, "memory.swap.max": 0, "memory.swap.current": 0,
                       "pids.max": 64, "pids.current": len(members)}, "cpu_max": [100000, 100000],
            "cpu_stat": {"usage_usec": max(0, number), "user_usec": max(0, number), "system_usec": 0},
            "pids_events": {"max": 0}, "members": sorted(members), "initial": initial,
            "network": service.read_network(lambda path, limit: network_raws()[path.rsplit("/", 1)[1]])}


def complete_service_records(service):
    # Core sampler controls/records are emitted by the actual sampler function.
    with SamplerFlow(service) as flow:
        if service.sampler(NONCE, CLI, 1) != 0: raise AssertionError("controlled sampler did not finish")
        raw = copy.deepcopy(flow.records)
    first = raw[0]["facts"]["read_bracket_ns"][0]
    initial = metric_record(service, first - 1000, [1], -1, True)
    ready = {"kind": "READY", "nonce": NONCE, "identity": SERVER, "started_at": "2026-10-28T00:00:00Z",
             "clock": clock_record(1, first - 1000, "sg32-keeper-before"), "initial": initial, "state": {"bytes": 0, "entries": 1, "bracket_ns": [first, first]}}
    checks = [{"kind": "STATE_CHECK", "nonce": NONCE, "phase": phase, "bytes": i,
               "entries": i + 1, "bracket_ns": [raw[1]["facts"]["read_bracket_ns"][1] + i, raw[1]["facts"]["read_bracket_ns"][1] + i]}
              for i, phase in enumerate(service.PHASES)]
    last = raw[-2]["facts"]["read_bracket_ns"][1]
    final = {"kind": "FINAL", "nonce": NONCE, "facts": metric_record(service, last + 100, [1], 4),
             "node_exit": 0, "sampler_exit": 0, "clock": clock_record(1, last + 120, "sg32-keeper-after"),
             "sampler_final": raw[-2], "sampler_sealed": raw[-1], "keeper_process": process_record(1),
             "state": {"bytes": 3, "entries": 4, "bracket_ns": [last + 50, last + 60]},
             "checked_phases": list(service.PHASES), "keeper_exit_tail": "UNKNOWN"}
    return raw[:3] + [ready] + checks + raw[3:] + [final]


class RealFD:
    """Only own ordinary files, with module-local proc/syscall boundaries."""
    def __init__(self, service):
        self.s = service
        self.temp = tempfile.TemporaryDirectory(prefix="sg32-pure-")
        self.root = Path(self.temp.name).resolve(strict=True)
        self.live, self.opened, self.closed = set(), [], []
        self.clock, self.group_raw = 100, b"0::/\n"
        self.stack = contextlib.ExitStack()

    def tick(self):
        self.clock += 10
        return self.clock

    def open(self, path, flags, mode=0o777, *, dir_fd=None):
        fd = os.open(self.translate(path), flags, mode, dir_fd=dir_fd)
        self.live.add(fd)
        self.opened.append(fd)
        return fd

    def close(self, fd):
        os.close(fd)
        self.live.remove(fd)
        self.closed.append(fd)

    def translate(self, path):
        return path

    def read_file(self, path, limit):
        raise AssertionError("unexpected proc read " + str(path))

    def __enter__(self):
        fake_os = types.SimpleNamespace(**vars(os))
        fake_os.open, fake_os.close = self.open, self.close
        clock = types.SimpleNamespace(**vars(self.s.clockprobe))
        clock.boottime, clock.read_file = self.tick, self.read_file
        self.stack.enter_context(mock.patch.object(self.s, "os", fake_os))
        self.stack.enter_context(mock.patch.object(self.s, "clockprobe", clock))
        return self

    def __exit__(self, *args):
        self.stack.close()
        for fd in tuple(self.live):
            os.close(fd)
            self.live.remove(fd)
        for path in self.root.rglob("*"):
            if path.is_dir() and not path.is_symlink():
                path.chmod(0o700)
        self.root.chmod(0o700)
        self.temp.cleanup()


class CgroupFD(RealFD):
    def __init__(self, service):
        super().__init__(service)
        self.directory = self.root / "cg"
        self.directory.mkdir()
        self.values = {"memory.max": b"512000000\n", "memory.current": b"1000\n",
                       "memory.peak": b"2000\n", "memory.swap.max": b"0\n",
                       "memory.swap.current": b"0\n", "pids.max": b"64\n",
                       "pids.current": b"3\n", "cpu.max": b"100000 100000\n",
                       "cpu.stat": b"usage_usec 9\nuser_usec 5\nsystem_usec 4\n",
                       "pids.events": b"max 0\n", "cgroup.procs": b"1\n2\n42\n",
                       "memory.events": b"low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"}
        for name, value in self.values.items():
            (self.directory / name).write_bytes(value)

    def write(self, name, value):
        (self.directory / name).write_bytes(value)

    def read_file(self, path, limit):
        if path.startswith("/proc/net/"):
            return network_raws()[path.rsplit("/", 1)[1]]
        if path == "/proc/self/cgroup":
            return self.group_raw
        if path == "/proc/self/mountinfo":
            return ("31 20 0:44 / " + str(self.directory) + " rw - cgroup2 cgroup rw\n").encode()
        raise AssertionError("unexpected proc path " + str(path))


class ProcessFD(RealFD):
    def __init__(self, service):
        super().__init__(service)
        self.directory = self.root / "pid"
        self.directory.mkdir()
        (self.directory / "ns").mkdir()
        (self.directory / "exe").write_bytes(b"\x7fELF controlled owned executable bytes\n")
        (self.directory / "ns/time").write_bytes(b"ordinary FD, not nsfs\n")
        os.link(self.directory / "ns/time", self.directory / "ns/time_for_children")
        st = (self.directory / "ns/time").stat()
        self.domain = [st.st_dev, st.st_ino]
        self.sha = hashlib.sha256((self.directory / "exe").read_bytes()).hexdigest()
        self.issue = None

    def translate(self, path):
        if type(path) is str and (path == "/proc/42" or path.startswith("/proc/42/")):
            suffix = path[len("/proc/42"):].lstrip("/")
            if self.issue == "zombie-exe" and suffix == "exe":
                raise FileNotFoundError(2, "controlled vanished exe", path)
            return str(self.directory / suffix)
        return path

    def read_file(self, path, limit):
        if self.issue == "gone":
            raise FileNotFoundError(2, "controlled process gone", path)
        if self.issue in ("eio", "eperm"):
            raise OSError(5 if self.issue == "eio" else 1, "controlled read error", path)
        if path == "/proc/42/stat":
            return proc_stat(birth=222 if self.issue == "birth" else 111)
        if path == "/proc/42/status":
            raw = proc_status()
            return raw.replace(b"501 501 501 501", b"502 502 502 502") if self.issue == "uid" else raw
        if path == "/proc/42/cgroup":
            return b"0::/moved\n" if self.issue == "group" else self.group_raw
        raise AssertionError("unexpected proc path " + str(path))


class SamplerFlow:
    """Actual sampler state machine; controlled parents, proc and selectors."""
    def __init__(self, service, issue=None):
        self.s, self.issue = service, issue
        self.now, self.selects = 10000000000, 0
        self.line, self.records = None, []
        self.node_started, self.node_gone, self.reaped = False, False, False
        self.final_hashes, self.last_end = 0, self.now
        self.temp = tempfile.TemporaryDirectory(prefix="sg32-sampler-flow-")
        self.exe = Path(self.temp.name) / "input"
        self.exe.write_bytes(b"inert source fixture\n")
        self.stack = contextlib.ExitStack()

    def emit(self, value):
        self.records.append(copy.deepcopy(value))
        if value["kind"] in ("SAMPLER_READY", "SAMPLE"):
            self.last_end = value["facts"]["read_bracket_ns"][1]
        if value["kind"] == "STOPPING_ACK":
            self.node_gone = True
        if value["kind"] == "NODE_BOUND" and self.issue == "unannounced-loss":
            self.node_gone = True

    def select(self, seconds):
        step, self.selects = self.selects, self.selects + 1
        packet = None
        if step == 0:
            if self.issue == "node-bind-gap":
                self.now += 2000000001
                return []
            packet = {"kind": "NODE", "nonce": NONCE, "pid": 3, "start_ticks": 111}
            self.node_started = True
        elif step == 1 or (step == 3 and self.issue != "duplicate-stopping"):
            self.now += int(seconds * 1000000000)
            return []
        elif step == 2 and self.issue == "unannounced-loss":
            self.now += int(seconds * 1000000000)
            return []
        elif step == 2 and self.issue == "reap-before-stopping":
            packet = {"kind": "REAP", "nonce": NONCE, "pid": 3, "start_ticks": 111, "exit": 0, "at_ns": self.now}
        elif step == 2 or (step == 3 and self.issue == "duplicate-stopping"):
            self.now += 1000
            packet = {"kind": "STOPPING", "nonce": NONCE}
        elif step == 4:
            if self.issue == "reap-edge-gap":
                self.now = self.last_end + 2000000002
            packet = {"kind": "REAP", "nonce": NONCE, "pid": 3,
                      "start_ticks": 222 if self.issue == "wrong-reap-birth" else 111,
                      "exit": 0, "at_ns": self.now - 1}
            self.reaped = True
        else:
            raise AssertionError("unexpected sampler select after finite scenario")
        self.line = json.dumps(packet).encode() + b"\n"
        return [(None, 1)]

    def readline(self, limit):
        if self.line is None:
            raise AssertionError("source requested unselected control")
        raw, self.line = self.line, None
        if len(raw) > limit:
            raise AssertionError("fixture exceeded actual bound")
        return raw

    def __enter__(self):
        fixture = self
        class Group:
            raw = b"0::/\n"
            def __enter__(self): return self
            def __exit__(self, *_): return None
            def snapshot(self, initial=False):
                begin = fixture.now
                fixture.now += 10
                members = [1, 2] if not fixture.node_started or fixture.node_gone else [1, 2, 3]
                number = len([r for r in fixture.records if r["kind"] in ("SAMPLER_READY", "SAMPLE")])
                return metric_record(fixture.s, begin, members, number, initial)
        class Process:
            def __init__(self, pid, group, domain, expected_exe=None, sha=None):
                self.pid, self.process = pid, {"pid": pid, "ppid": 0 if pid == 1 else 1, "start_ticks": 111}
            def __enter__(self): return self
            def __exit__(self, *_): return None
            def read(self, pending_exit=False):
                if self.pid == 3 and fixture.node_gone and not pending_exit:
                    raise fixture.s.Fault("unannounced-node-loss")
                result = process_record(self.pid)
                if self.pid == 3 and fixture.node_gone:
                    result = {"pid": self.pid, "process": self.process, "available": False,
                              "reason": "announced-exit-pending-owned-reap", "rss": None, "hwm": None, "pss": None}
                return result
            def final_hash(self):
                fixture.final_hashes += 1
                return {"pid": 3, "process": self.process, "exe_sha256": CLI, "namespace": [5, 901]}
        class Selector:
            def __enter__(self): return self
            def __exit__(self, *_): return None
            def register(self, *_): return None
            def select(self, seconds): return fixture.select(seconds)
        fake_os = types.SimpleNamespace(**vars(os))
        fake_os.getpid, fake_os.getppid = lambda: 2, lambda: 1
        fake_os.getuid, fake_os.getgid = lambda: 501, lambda: 20
        def node_stat(path, **kw):
            if path != "/inputs/corsad" or kw != {"follow_symlinks": False}:
                raise AssertionError("unexpected stat boundary")
            return os.stat(self.exe)
        fake_os.stat = node_stat
        clock = types.SimpleNamespace(**vars(self.s.clockprobe))
        clock.boottime = lambda: self.now
        self.stack.enter_context(mock.patch.object(self.s, "os", fake_os))
        self.stack.enter_context(mock.patch.object(self.s, "clockprobe", clock))
        self.stack.enter_context(mock.patch.object(self.s, "Cgroup", Group))
        self.stack.enter_context(mock.patch.object(self.s, "ProcWitness", Process))
        self.stack.enter_context(mock.patch.object(self.s, "selectors", types.SimpleNamespace(DefaultSelector=Selector, EVENT_READ=1)))
        self.stack.enter_context(mock.patch.object(self.s, "sys", types.SimpleNamespace(stdin=types.SimpleNamespace(buffer=types.SimpleNamespace(readline=self.readline)))))
        self.stack.enter_context(mock.patch.object(self.s, "emit", self.emit))
        self.stack.enter_context(mock.patch.object(self.s, "qualified_clock", lambda role, phase, nonce: clock_record(2, self.now, phase)))
        return self

    def __exit__(self, *_):
        self.stack.close()
        self.temp.cleanup()


class PreparerFiles(RealFD):
    """Real own fresh files; root-only chown boundaries are recorded, not run."""
    def __init__(self, service):
        super().__init__(service)
        self.paths = {name: self.root / name for name in ("inputs", "auth", "work")}
        for path in self.paths.values(): path.mkdir()
        self.config = {"nonce": NONCE, "files": {}}
        for name in service.INPUTS:
            raw = ("inert input " + name + "\n").encode()
            path = self.paths["inputs"] / name
            path.write_bytes(raw)
            path.chmod(0o600)
            self.config["files"][name] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        (self.paths["inputs"] / "manifest.json").write_bytes(json.dumps(self.config).encode() + b"\n")
        self.chowns, self.records = [], []

    def translate(self, path):
        if type(path) is str:
            for prefix, folder in (("/inputs", "inputs"), ("/run/c1", "auth"), ("/work", "work")):
                if path == prefix or path.startswith(prefix + "/"):
                    return str(self.paths[folder] / path[len(prefix):].lstrip("/"))
        return path

    def read_file(self, path, limit):
        raw = Path(self.translate(path)).read_bytes()
        if len(raw) > limit: raise AssertionError("fixture read exceeds actual source bound")
        return raw

    def __enter__(self):
        super().__enter__()
        fake_os = types.SimpleNamespace(**vars(self.s.os))
        fake_os.getuid, fake_os.getgid = lambda: 0, lambda: 0
        fake_os.lstat = lambda path: os.lstat(self.translate(path))
        fake_os.listdir = lambda path: os.listdir(self.translate(path))
        fake_os.chmod = lambda path, mode: os.chmod(self.translate(path), mode)
        fake_os.mkdir = lambda path, mode: os.mkdir(self.translate(path), mode)
        fake_os.chown = lambda path, uid, gid: self.chowns.append((str(path), uid, gid))
        fake_os.fchown = lambda fd, uid, gid: self.chowns.append((fd, uid, gid))
        self.stack.enter_context(mock.patch.object(self.s, "os", fake_os))
        raw = json.dumps(self.config).encode() + b"\n"
        self.stack.enter_context(mock.patch.object(self.s, "sys", types.SimpleNamespace(stdin=types.SimpleNamespace(buffer=io.BytesIO(raw)))))
        self.stack.enter_context(mock.patch.object(self.s, "time", types.SimpleNamespace(monotonic=lambda: 100.)))
        self.stack.enter_context(mock.patch.object(self.s.secrets, "token_hex", lambda size: "c" * (2 * size)))
        self.stack.enter_context(mock.patch.object(self.s, "emit", lambda value: self.records.append(copy.deepcopy(value))))
        return self


class SG32PureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c, cls.clock, cls.s, cls.h = subjects()

    def test_01_strict_json_and_real_owned_bounded_reader(self):
        c = self.c
        self.assertEqual(c.strict_json(b'{"zero":0}'), {"zero": 0})
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}'):
            with self.subTest(json=raw), self.assertRaises((c.Fault, ValueError)):
                c.strict_json(raw)
        with tempfile.TemporaryDirectory(prefix="sg32-owned-") as folder:
            path = Path(folder) / "input"
            path.write_bytes(b"owned\n")
            path.chmod(0o600)
            self.assertEqual(c.read_owned(path, 6), b"owned\n")
            with self.assertRaises(c.Fault):
                c.read_owned(path, 5)
            path.chmod(0o622)
            with self.assertRaises(c.Fault):
                c.read_owned(path, 6)
            path.chmod(0o600)
            link = Path(folder) / "alias"
            link.symlink_to(path)
            with self.assertRaises((c.Fault, OSError)):
                c.read_owned(link, 6)
            with self.assertRaises(c.Fault):
                c.encode({"body": "x" * 100}, 32)

    def test_02_actual_driver_source_bound_workload_and_receipts(self):
        rows = driver_rows()
        result = self.c.verify_driver([json.dumps(r).encode() + b"\n" for r in rows], NONCE, SERVER)
        self.assertEqual(result["phases"], [32, 16, 32, 0])
        self.assertEqual(result["identities"], 48)
        self.assertEqual(result["source_bound_useful_sequences"], 224)
        self.assertEqual(result["source_bound_primary_pairs"], 672)
        self.assertFalse(result["full_terminal_per_peer_RAW"])
        self.assertEqual([p["useful_exchanges"] for p in rows[0]["peers"][:16]], [1] * 16)
        self.assertEqual(rows[3]["peers"], None)

    def test_03_driver_incomplete_reuse_underwork_or_bad_authority_refuses(self):
        for case in ("order", "nonce", "reuse", "ordinal", "underwork", "undercharge", "rollback", "cached", "zero"):
            rows = driver_rows()
            if case == "order": rows[0], rows[1] = rows[1], rows[0]
            elif case == "nonce": rows[2]["nonce"] = "c" * 32
            elif case == "reuse": rows[2]["peers"][-1]["identity"] = rows[0]["peers"][0]["identity"]
            elif case == "ordinal": rows[1]["peers"][0]["ordinal"] = True
            elif case == "underwork": rows[2]["peers"][0]["useful_exchanges"] -= 1
            elif case == "undercharge": rows[3]["application_charge"]["frames"] -= 1
            elif case == "rollback": rows[3]["application_charge"]["bytes"] = 0
            elif case == "cached": rows[3]["cached_live_acceptance"] = True
            else: rows[3]["verified_channels"] = 1
            with self.subTest(driver=case), self.assertRaises((self.c.Fault, ValueError)):
                self.c.verify_driver(rows, NONCE, SERVER)

    def test_04_mandatory_scalar_counter_and_cgroup_mount_parsing(self):
        s = self.s
        self.assertEqual(s.uint(b"0\n"), 0)
        self.assertEqual(s.uint(b"512000000"), 512000000)
        for raw in (b"", b"-1\n", b"00\n", b"max\n", b"18446744073709551616\n", True):
            with self.subTest(uint=raw), self.assertRaises(s.Fault):
                s.uint(raw)
        for raw in (b"oom 0\noom 0\n", b"oom -1\n", b"high 0\n", b"oom 0"):
            with self.subTest(counters=raw), self.assertRaises(s.Fault):
                s.kv(raw, {"oom"})
        mount = b"31 20 0:44 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
        self.assertEqual(s.cg_path(b"0::/\n", mount), ("/sys/fs/cgroup", "/"))
        with self.assertRaises(s.Fault): s.cg_path(b"0::/../escape\n", mount)
        with self.assertRaises(s.Fault): s.cg_path(b"0::/\n", mount + mount)

    def test_05_actual_cgroup_fd_read_brackets_and_counter_refusals(self):
        s = self.s
        with CgroupFD(s) as fixture:
            with s.Cgroup() as group:
                first = group.snapshot(initial=True)
                self.assertEqual(first["peak"], 2000)
                self.assertEqual(first["members"], [1, 2, 42])
                self.assertEqual(first["limits"]["memory.max"], 512000000)
                self.assertLessEqual(first["peak_bracket_ns"][1], first["current_bracket_ns"][0])
                # current is a later, separate read; it need not be<=peak.
                fixture.write("memory.current", b"2500\n")
                separate = group.snapshot()
                self.assertEqual((separate["peak"], separate["current"]), (2000, 2500))
                fixture.write("memory.peak", b"3000\n")
                self.assertEqual(group.snapshot()["peak"], 3000)
                fixture.write("memory.peak", b"2000\n")
                with self.assertRaisesRegex(s.Fault, "counter-regression"):
                    group.snapshot()
            self.assertFalse(fixture.live)
        for name, raw, adverse in (("memory.max", b"600000000\n", False), ("memory.swap.max", b"1\n", False),
                                   ("memory.events", b"low 0\nhigh 0\nmax 0\noom 1\noom_kill 0\noom_group_kill 0\n", True),
                                   ("memory.peak", b"512000001\n", True), ("memory.current", b"\n", False)):
            with self.subTest(metric=name), CgroupFD(s) as fixture:
                fixture.write(name, raw)
                with s.Cgroup() as group, self.assertRaises(s.Adverse if adverse else s.Fault):
                    group.snapshot(initial=True)
                self.assertFalse(fixture.live)
        with CgroupFD(s) as fixture:
            with s.Cgroup() as group:
                (fixture.directory / "memory.peak").rename(fixture.directory / "oldpeak")
                fixture.write("memory.peak", b"2000\n")
                with self.assertRaisesRegex(s.Fault, "metric-name-changed"):
                    group.snapshot()
            self.assertFalse(fixture.live)

    def test_06_actual_process_birth_namespace_held_hash_and_pending_exit(self):
        s = self.s
        with ProcessFD(s) as fixture:
            with s.ProcWitness(42, fixture.group_raw, fixture.domain, sha=fixture.sha) as process:
                self.assertTrue(process.read()["available"])
                fixture.issue = "zombie-exe"
                with self.assertRaises(FileNotFoundError): process.read(False)
                missing = process.read(True)
                self.assertFalse(missing["available"])
                self.assertIsNone(missing["rss"])
                self.assertEqual(process.final_hash()["exe_sha256"], fixture.sha)
            self.assertFalse(fixture.live)
        for issue in ("birth", "uid", "group", "eio", "eperm"):
            with self.subTest(process=issue), ProcessFD(s) as fixture:
                with s.ProcWitness(42, fixture.group_raw, fixture.domain) as process:
                    fixture.issue = issue
                    with self.assertRaises((s.Fault, OSError)): process.read(True)
                self.assertFalse(fixture.live)
        with ProcessFD(s) as fixture:
            with s.ProcWitness(42, fixture.group_raw, fixture.domain) as process:
                (fixture.directory / "ns/time").unlink()
                (fixture.directory / "ns/time").write_bytes(b"new namespace inode")
                with self.assertRaisesRegex(s.Fault, "named-binding-changed"): process.read(True)
            self.assertFalse(fixture.live)

    def test_07_secret_split_output_environment_and_state_walk(self):
        s, emitted = self.s, []
        secret = {"username": "sg32_user", "password": "CANARY_SECRET"}
        child = types.SimpleNamespace(buffers={"out": bytearray()})
        with mock.patch.object(s, "emit", lambda value: emitted.append(copy.deepcopy(value))):
            s.log_chunk(child, "out", b"ordinary-prefix CANARY_", secret, NONCE)
            with self.assertRaisesRegex(s.Fault, "secret-in-node-output"):
                s.log_chunk(child, "out", b"SECRET", secret, NONCE)
        self.assertFalse(any(b"CANARY_SECRET" in base64.b64decode(x["raw_base64"]) for x in emitted))
        env = s.daemon_environment(secret)
        self.assertEqual(env["GOMAXPROCS"], "1")
        for name in ("GOMEMLIMIT", "GOGC", "CORSA_MEM_LIMIT_BYTES"):
            self.assertNotIn(name, env)
        self.assertEqual(env["CORSA_BOOTSTRAP_PEERS"], "127.0.0.1:64647")
        with RealFD(s) as fixture:
            (fixture.root / "data").mkdir()
            (fixture.root / "data/file").write_bytes(b"abc")
            fixture.translate = lambda path: str(fixture.root) if path == "/work" else path
            result = s.state_walk()
            self.assertEqual(result["bytes"], 3)
            self.assertEqual(result["scope"], "bounded-nonatomic-logical-state-walk")
            (fixture.root / "alias").symlink_to(fixture.root / "data/file")
            with self.assertRaises(s.Fault): s.state_walk()
            self.assertFalse(fixture.live)

    def test_08_actual_sampler_owned_stopping_reap_and_coverage_edges(self):
        s = self.s
        with SamplerFlow(s) as fixture:
            self.assertEqual(s.sampler(NONCE, CLI, 1), 0)
            kinds = [r["kind"] for r in fixture.records]
            self.assertEqual(kinds, ["SAMPLER_READY", "SAMPLE", "NODE_BOUND", "STOPPING_ACK", "SAMPLE", "SAMPLER_FINAL", "SAMPLER_SEALED"])
            final = fixture.records[-2]
            self.assertEqual(final["ordinal"], 3)
            self.assertEqual(final["node_reap"]["exit"], 0)
            self.assertLessEqual(final["node_reap"]["at_ns"] - final["node_reap_edge_end_ns"], 2000000000)
            self.assertEqual(fixture.final_hashes, 1)
            pending = fixture.records[-3]
            self.assertFalse(pending["processes"][-1]["available"])
            self.assertIsNone(pending["processes"][-1]["rss"])
        for issue in ("node-bind-gap", "unannounced-loss", "reap-before-stopping", "wrong-reap-birth", "reap-edge-gap", "duplicate-stopping"):
            with self.subTest(sampler=issue), SamplerFlow(s, issue) as fixture:
                with self.assertRaises(s.Fault): s.sampler(NONCE, CLI, 1)
                self.assertFalse(any(r["kind"] == "SAMPLER_SEALED" for r in fixture.records))
                self.assertEqual(fixture.final_hashes, 0)

    def test_09_actual_preparer_fresh_owned_inputs_secret_and_refusals(self):
        s = self.s
        with PreparerFiles(s) as fixture:
            self.assertEqual(s.preparer(NONCE), 0)
            self.assertEqual([r["kind"] for r in fixture.records], ["PREPARED"])
            secret = fixture.paths["auth"] / "auth.json"
            value = json.loads(secret.read_bytes())
            self.assertEqual(value["password"], "c" * 64)
            self.assertEqual(stat.S_IMODE(secret.stat().st_mode), 0o600)
            self.assertFalse(any(value["password"] in json.dumps(row) for row in fixture.records))
            for name in ("data", "home", "tmp"):
                self.assertEqual(stat.S_IMODE((fixture.paths["work"] / name).stat().st_mode), 0o700)
                self.assertIn(("/work/" + name, 501, 20), fixture.chowns)
            self.assertFalse(fixture.live)
        for issue in ("existing-secret", "bad-input", "symlink-input"):
            with self.subTest(preparer=issue), PreparerFiles(s) as fixture:
                if issue == "existing-secret":
                    old = fixture.paths["auth"] / "auth.json"
                    old.write_bytes(b"never overwrite existing credentials")
                elif issue == "bad-input":
                    (fixture.paths["inputs"] / "corsad").write_bytes(b"wrong bytes")
                else:
                    path = fixture.paths["inputs"] / "corsad"
                    path.unlink()
                    path.symlink_to(fixture.paths["inputs"] / "c2driver")
                with self.assertRaises((s.Fault, OSError)): s.preparer(NONCE)
                self.assertFalse(fixture.records)
                if issue == "existing-secret":
                    self.assertEqual(old.read_bytes(), b"never overwrite existing credentials")
                self.assertFalse(fixture.live)

    def test_10_actual_output_limits_clock_refusal_and_cleanup_failure(self):
        s = self.s
        with mock.patch.object(s.clockprobe, "capture", return_value={"complete": True, "supported_source_guard": False, "shared_time_domain": False}):
            with self.assertRaisesRegex(s.Fault, "clock-domain-unqualified"):
                s.qualified_clock("keeper", "before", NONCE)
        with mock.patch.object(s, "OUTPUT", s.RAW_LIMIT), mock.patch.object(s, "sys", types.SimpleNamespace(stdout=types.SimpleNamespace(buffer=io.BytesIO()))):
            with self.assertRaises(s.Fault): s.emit({"kind": "record"})
        with tempfile.TemporaryDirectory(prefix="sg32-close-") as folder:
            fds = [os.open(Path(folder) / str(i), os.O_RDWR | os.O_CREAT, 0o600) for i in range(3)]
            closed = []
            fake_os = types.SimpleNamespace(**vars(os))
            def close(fd):
                os.close(fd)
                closed.append(fd)
                if fd == fds[1]: raise OSError("controlled postclose failure")
            fake_os.close = close
            with mock.patch.object(s, "os", fake_os), self.assertRaises(OSError):
                s.close_fds(fds)
            self.assertEqual(set(closed), set(fds))
        # Keep the primary error when __exit__ also cannot certify cleanup.
        group = object.__new__(s.Cgroup)
        def failed_close(): raise OSError("controlled cleanup uncertainty")
        group.close = failed_close
        primary = s.Fault("original-read-failure")
        group.__exit__(s.Fault, primary, None)
        self.assertTrue(primary.cleanup_failed)
        self.assertEqual(str(primary), "original-read-failure")


    def test_11_actual_loopback_network_raw_parser_and_provenance(self):
        c = self.c
        raws = network_raws()
        # Linux renders IPv6 socket addresses as little-endian32bit words.
        mapped_loop = "0000000000000000FFFF00000100007F"
        raws["tcp6"] += ("0: " + mapped_loop + ":FC86 " + mapped_loop + ":A001 01 00000000:00000000 00:00000000 00000000 501 0 1\n").encode()
        proof = c.read_network(lambda path, limit: raws[path.rsplit("/", 1)[1]])
        self.assertTrue(proof["loopback_only_observed"])
        self.assertEqual(proof["socket_rows"], 1)
        self.assertEqual(c.verify_network(proof)["socket_rows"], 1)
        bad = copy.deepcopy(proof)
        bad["raw"]["tcp6"]["sha256"] = "0" * 64
        with self.assertRaises(c.Fault): c.verify_network(bad)
        for case in ("v4-address", "v6-address", "v4-route", "v6-route", "socket", "mapped-external", "wildcard-listener", "header", "missing-file", "mask-zero", "mask-noncontiguous"):
            bad = network_raws()
            if case == "v4-address": bad["fib_trie"] = b" |-- 10.0.0.1\n /32 host LOCAL\n"
            elif case == "v6-address": bad["if_inet6"] = b"fe800000000000000000000000000001 01 40 20 80 eth0\n"
            elif case == "v4-route": bad["route"] += b"eth0 00000000 0100000A 0003 0 0 0 00000000 0 0 0\n"
            elif case == "v6-route": bad["ipv6_route"] = b"00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00000001 lo\n"
            elif case in ("socket", "wildcard-listener"):
                local = b"0100000A:FC86" if case == "socket" else b"00000000:FC86"
                bad["tcp"] += b"0: " + local + b" 00000000:0000 0A 00000000:00000000 00:00000000 00000000 501 0 1\n"
            elif case == "mapped-external":
                bad["tcp6"] += b"0: 0000000000000000FFFF00000100000A:FC86 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000 501 0 1\n"
            elif case == "header": bad["tcp"] = b"unrecognised table header\n"
            elif case.startswith("mask-"):
                mask = b"00000000" if case == "mask-zero" else b"00FF00FF"
                bad["route"] += b"lo 0000007F 00000000 0001 0 0 0 " + mask + b" 0 0 0\n"
            else: del bad["udp6"]
            with self.subTest(network=case), self.assertRaises((c.Fault, ValueError, KeyError)):
                c.network_values(bad)

    def test_12_actual_container_metadata_isolation_budgets_and_ownership(self):
        h = self.h
        volumes = h.volume_names(NONCE)
        for role in ("preparer", "service", "driver"):
            meta = container_meta(role, volumes)
            self.assertIsNone(h.verify_created(meta, role, NONCE, volumes, "1" * 64))
        for case in ("foreign", "memory", "swap", "capability", "network", "writable-secret", "host-bind", "missing-mount", "rw-int", "budget-bool", "driver-empty-C2A"):
            role = "driver" if case == "driver-empty-C2A" else "service"
            meta = container_meta(role, volumes)
            if case == "foreign": meta["Config"]["Labels"]["corsa.sg32.owner"] = "c" * 32
            elif case == "memory": meta["HostConfig"]["Memory"] += 1
            elif case == "swap": meta["HostConfig"]["MemorySwap"] += 1
            elif case == "capability": meta["HostConfig"]["CapAdd"] = ["SYS_ADMIN"]
            elif case == "network": meta["HostConfig"]["NetworkMode"] = "bridge"
            elif case == "writable-secret": meta["Mounts"][1]["RW"] = True
            elif case == "host-bind": meta["HostConfig"]["Binds"] = ["/foreign:/data"]
            elif case == "missing-mount": meta["Mounts"].pop()
            elif case == "rw-int": meta["Mounts"][1]["RW"] = 0
            elif case == "budget-bool": meta["HostConfig"]["NanoCpus"] = True
            else: meta["Config"]["Env"].append("CORSA_C2A_RUN=")
            with self.subTest(metadata=case), self.assertRaises((h.Fault, ValueError, KeyError)):
                h.verify_created(meta, role, NONCE, volumes, "1" * 64)

    def test_13_actual_static_ELF_and_admission_fail_before_resources(self):
        c, h = self.c, self.h
        raw = inert_elf()
        self.assertEqual(c.static_elf(raw), {"class": 64, "endian": "little", "machine": "aarch64", "dynamic": False})
        for field in ("architecture", "interpreter", "program-bound"):
            bad = bytearray(raw)
            if field == "architecture": struct.pack_into("<H", bad, 18, 62)
            elif field == "interpreter": struct.pack_into("<I", bad, 64, 3)
            else: struct.pack_into("<Q", bad, 32, 65536)
            with self.subTest(ELF=field), self.assertRaises(c.Fault): c.static_elf(bytes(bad))
        with tempfile.TemporaryDirectory(prefix="sg32-admission-") as folder:
            path = Path(folder) / "admission.json"
            qualification = Path(folder) / "qualification.json"
            qualification.write_bytes(b'{"status":"NOT_BUILT"}\n')
            qualification.chmod(0o600)
            cfg = {"profile": "SG32-v1", "owner_approved": False, "allow_recovery_hold": False,
                   "one_attempt": True, "inputs_ready": False, "files": {}}
            path.write_bytes(json.dumps(cfg).encode())
            path.chmod(0o600)
            with self.assertRaisesRegex(h.Fault, "INERT-owner-admission"): h.read_admission(path)
            cfg.update(owner_approved=True, allow_recovery_hold=True)
            path.write_bytes(json.dumps(cfg).encode())
            with self.assertRaisesRegex(h.Fault, "qualified-inputs-missing"): h.read_admission(path)
            cfg.update(inputs_ready=True, files={name: {} for name in c.INPUT_NAMES},
                       qualification_path=str(qualification), qualification_sha256=hashlib.sha256(qualification.read_bytes()).hexdigest())
            path.write_bytes(json.dumps(cfg).encode())
            with self.assertRaisesRegex(h.Fault, "qualified-artifact-provenance"): h.read_admission(path)
            self.assertFalse(list(Path(folder).glob("*CLAIM*")))

    def test_14_observed_early_node_exit_preserves_FAIL_and_birth_availability(self):
        s = self.s
        child = types.SimpleNamespace(p=types.SimpleNamespace(pid=42, poll=lambda: None))
        self.assertIsNone(s.node_exit_fact(child, None, "startup"))
        for code in (0, 7, -9):
            child.p.poll = lambda code=code: code
            fact = s.node_exit_fact(child, None, "startup")
            self.assertEqual(fact["returncode"], code)
            self.assertFalse(fact["birth_available"])
            self.assertIsNone(fact["process"])
            self.assertTrue(fact["early"])
            with self.subTest(exit=code), self.assertRaises(s.Adverse) as caught:
                s.node_running(child, None, "startup")
            self.assertEqual(caught.exception.facts, fact)
        birth = {"pid": 42, "ppid": 1, "start_ticks": 111}
        child.p.poll = lambda: 0
        fact = s.node_exit_fact(child, birth, "normal-stop", True)
        self.assertEqual(fact["process"], birth)
        self.assertFalse(fact["early"])
        self.assertTrue(fact["stop_requested"])

    def test_15_actual_service_reducer_full_sampler_transcript_and_causal_refusals(self):
        c, s = self.c, self.s
        records = complete_service_records(s)
        result = c.verify_service(records, NONCE, CLI)
        self.assertEqual(result["peak_bytes"], 2004)
        self.assertEqual(result["kernel_window_end"], "bounded-final-peak-read")
        self.assertEqual(result["keeper_exit_tail"], "UNKNOWN")
        self.assertEqual(result["isolated_role_charge"], "UNKNOWN")
        for case in ("missing-seal", "sample-ordinal", "reap-gap", "member", "birth", "namespace", "unannounced-missing", "stop-binding", "OOM", "events-before-peak", "exit", "tail", "clock", "NSpid"):
            bad = copy.deepcopy(records)
            kind = lambda name: next(r for r in bad if r["kind"] == name)
            sample = kind("SAMPLE")
            if case == "missing-seal": bad.remove(kind("SAMPLER_SEALED"))
            elif case == "sample-ordinal": sample["ordinal"] += 1
            elif case == "reap-gap": kind("SAMPLER_FINAL")["node_reap"]["at_ns"] += 3000000000
            elif case == "member": sample["facts"]["members"].append(99)
            elif case == "birth": sample["processes"][-1]["process"]["start_ticks"] += 1
            elif case == "namespace": sample["processes"][-1]["namespace"][1] += 1
            elif case == "unannounced-missing": sample["processes"][-1]["available"] = False
            elif case == "stop-binding": kind("STOPPING_ACK")["start_ticks"] += 1
            elif case == "OOM": kind("FINAL")["facts"]["events_after"]["oom_kill"] = 1
            elif case == "events-before-peak": kind("FINAL")["facts"]["peak_bracket_ns"][0] = kind("FINAL")["facts"]["read_bracket_ns"][0] - 1
            elif case == "exit": kind("FINAL")["node_exit"] = 7
            elif case == "tail": kind("FINAL")["keeper_exit_tail"] = "KNOWN_ZERO"
            elif case == "NSpid": del sample["processes"][-1]["status"]["NSpid"]
            else: kind("SAMPLER_SEALED")["clock"]["contexts"]["thread_self"]["time"][1] += 1
            with self.subTest(reducer=case), self.assertRaises((c.Fault, KeyError, ValueError)):
                c.verify_service(bad, NONCE, CLI)

    def test_16_known_adverse_cgroup_prefix_survives_later_network_failure(self):
        s = self.s
        for case in ("OOM", "peak", "clean-network-error"):
            with self.subTest(prefix=case), CgroupFD(s) as fixture:
                if case == "OOM": fixture.write("memory.events", b"low 0\nhigh 0\nmax 0\noom 1\noom_kill 0\noom_group_kill 0\n")
                elif case == "peak": fixture.write("memory.peak", b"512000001\n")
                with s.Cgroup() as group:
                    with mock.patch.object(s, "read_network", side_effect=s.Fault("controlled later missing network")):
                        if case == "clean-network-error":
                            with self.assertRaises(s.Fault) as caught: group.snapshot(initial=True)
                            self.assertNotIsInstance(caught.exception, s.Adverse)
                        else:
                            with self.assertRaises(s.Adverse) as caught: group.snapshot(initial=True)
                            self.assertFalse(caught.exception.facts["complete"])
                            self.assertEqual(caught.exception.facts["scope"], "INCOMPLETE_CGROUP_PREFIX")
                self.assertFalse(fixture.live)

    def test_17_actual_engine_command_closes_completed_not_unknown_and_bounds_records(self):
        h = self.h
        for case in ("normal", "nonzero", "unknown"):
            engine = object.__new__(h.Engine)
            engine.uncertain, engine.output_failed, engine.live, engine.commands = False, False, [], []
            closed = []
            class CommandChild:
                def __init__(self, owner, args, role):
                    self.number, self.out, self.err = 0, bytearray(b"{}"), bytearray()
                    self.p = types.SimpleNamespace(returncode=7 if case == "nonzero" else None if case == "unknown" else 0)
                    owner.live.append(self)
                def done(self): return case != "unknown"
                def close(self, deadline): closed.append(self); return True
            def pump(predicate, deadline):
                if not predicate(): raise h.Fault("controlled unknown CLI deadline")
            engine.pump_until = pump
            with self.subTest(command=case), mock.patch.object(h, "Child", CommandChild), mock.patch.object(h.time, "monotonic", return_value=100.0):
                if case == "normal": self.assertEqual(engine.command(["owned-query"], 101, True), b"{}")
                else:
                    with self.assertRaises(h.Fault): engine.command(["owned-query"], 101, True)
                self.assertEqual(len(closed), 0 if case == "unknown" else 1)
                self.assertEqual(len(engine.live), 1)
                if case == "unknown": self.assertTrue(engine.uncertain)
        child = object.__new__(h.Child)
        child.out = bytearray(b'{"kind":"READY"}\npartial')
        self.assertEqual(child.records(), [{"kind": "READY"}])
        with self.assertRaises(h.Fault): child.records(True)
        child.out = bytearray(b"x" * 65537)
        with self.assertRaises(h.Fault): child.records()
        child.out = bytearray(b'{"malformed":\n')
        with self.assertRaises(ValueError): child.records()
        with mock.patch.object(h, "read_admission", side_effect=h.Fault("qualified-inputs-missing")), mock.patch.object(h, "Engine", side_effect=AssertionError("no Docker before admission")):
            with self.assertRaises(h.Fault): h.run(Path("not-admitted"))

    def test_18_owned_cleanup_refuses_foreign_and_unknown_HOLD_survives_bad_primary_RAW(self):
        h = self.h
        ident = "2" * 64
        for case in ("own", "foreign", "uncertain"):
            class CleanupEngine:
                def __init__(self): self.uncertain, self.owned, self.removed = case == "uncertain", True, []
                def close_children(self, deadline): return True
                def command(self, args, deadline, mutation=False):
                    if args[0] == "ps": return (ident + "\n").encode() if self.owned else b""
                    if args[:2] == ["volume", "ls"]: return b""
                    if args[0] == "rm": self.removed.append(args[-1]); self.owned = False; return (ident + "\n").encode()
                    raise AssertionError("unexpected cleanup command")
                def inspect(self, owned_id, deadline):
                    meta = container_meta("service", h.volume_names(NONCE))
                    if case == "foreign": meta["Config"]["Labels"]["corsa.sg32.owner"] = "d" * 32
                    return meta
            engine = CleanupEngine()
            with self.subTest(cleanup=case):
                self.assertEqual(h.cleanup(engine, NONCE, [ident], [], 101), case == "own")
                self.assertEqual(engine.removed, [] if case == "foreign" else [ident])
        class HoldObserved(BaseException): pass
        with tempfile.TemporaryDirectory(prefix="sg32-unknown-") as folder:
            parent = Path(folder).resolve(strict=True)
            runs = parent / "runs"; runs.mkdir(mode=0o700)
            admission = parent / "admission.json"
            raw = b"controlled approved source fixture\n"
            cfg = {"run_parent": str(runs), "files": {name: {"bytes": 1} for name in self.c.INPUT_NAMES}}
            malformed = object.__new__(h.Child)
            malformed.role, malformed.out = "service", bytearray(b'{"malformed":\n')
            malformed.p = types.SimpleNamespace(pid=555, poll=lambda: None)
            class UnknownEngine:
                def __init__(self):
                    self.live, self.commands, self.root = [malformed], [], None
                    self.uncertain, self.output_failed, self.result = False, False, None
                    self.selector = types.SimpleNamespace(close=lambda: None)
                def command(self, args, deadline, mutation=False):
                    self.commands.append(args)
                    if args[0] == "context": return json.dumps(h.ENDPOINT).encode()
                    if args[0] == "info": return b'"29.8.0"'
                    if args[:2] == ["image", "inspect"]:
                        return json.dumps({"Id": h.IMAGE, "Os": "linux", "Architecture": "arm64", "Config": {"Volumes": None}}).encode()
                    if args[:2] == ["volume", "ls"]: return b""
                    self.uncertain = True
                    raise h.Fault("primary-create-refusal")
                def put(self, name, value):
                    if name == "result.json":
                        self.result = copy.deepcopy(value)
                        raise OSError("controlled receipt failure")
                def close_children(self, deadline): return False
            engine = UnknownEngine()
            fake_os = types.SimpleNamespace(**vars(os))
            fake_os.getuid, fake_os.getgid, fake_os.getpgrp = lambda: 501, lambda: 20, lambda: 77
            fake_os.statvfs = lambda _: types.SimpleNamespace(f_bavail=32 * 1024**3, f_frsize=1)
            def read_owned(path, limit):
                if Path(path) == admission: return raw
                if Path(path) == h.LOCK / "owner": return b"name=d1-sg32\npgid=77\n"
                raise AssertionError("unexpected before-claim read")
            def hold(seconds):
                self.assertEqual(seconds, 3600)
                raise HoldObserved()
            output = io.StringIO()
            with mock.patch.object(h, "read_admission", return_value=(raw, cfg)), mock.patch.object(h, "read_owned", read_owned), mock.patch.object(h, "os", fake_os), mock.patch.object(h, "Engine", return_value=engine), mock.patch.object(h.secrets, "token_hex", return_value=NONCE), mock.patch.object(h, "time", types.SimpleNamespace(monotonic=lambda: 100., sleep=hold)), mock.patch.object(h, "cleanup", return_value=False), contextlib.redirect_stdout(output):
                with self.assertRaises(HoldObserved): h.run(admission)
            self.assertEqual(engine.result["error"]["reason"], "primary-create-refusal")
            self.assertEqual(engine.result["service_protocol"], "INCOMPLETE")
            self.assertEqual(engine.result["status"], "RECOVERY_REQUIRED")
            self.assertFalse(engine.result["measurement_qualified"])
            self.assertTrue((parent / "ONE-ATTEMPT-CLAIM.json").exists())
            self.assertIn("RECOVERY_REQUIRED", output.getvalue())

    def test_19_clock_raw_offsets_typed_proof_and_nonzero_values(self):
        c = self.c
        good = clock_record(2, 100, "sg32-sampler-before")
        raw = b"monotonic -3 10\nboottime 4 20\n"
        values = {"monotonic": {"seconds": -3, "nanoseconds": 10}, "boottime": {"seconds": 4, "nanoseconds": 20}}
        for row in good["contexts"].values():
            row["offsets_for_children"] = {"raw_base64": base64.b64encode(raw).decode(), "sha256": hashlib.sha256(raw).hexdigest(), "values": copy.deepcopy(values)}
        self.assertEqual(c.clock_values(good, NONCE), [5, 901])
        for case in ("proof-bool", "malformed-raw", "values-mismatch"):
            bad = copy.deepcopy(good)
            thread = bad["contexts"]["thread_self"]
            if case == "proof-bool": thread["offsets_reader"]["before"]["process"]["ppid"] = True
            elif case == "malformed-raw":
                raw_bad = b"monotonic 0 1000000000\nboottime 0 0\n"
                thread["offsets_for_children"].update(raw_base64=base64.b64encode(raw_bad).decode(), sha256=hashlib.sha256(raw_bad).hexdigest())
            else: thread["offsets_for_children"]["values"]["boottime"]["seconds"] += 1
            with self.subTest(clock=case), self.assertRaises((c.Fault, ValueError, KeyError)):
                c.clock_values(bad, NONCE)


    def test_20_actual_Docker_capability_spelling_preserves_exact_allowlist(self):
        # Captured command13 inspect of the created, never-started preparer.
        # Commands SHA f7daca458c014e256d0ecdc26bc435a01b623a84fabd6cbbb894706f7d5f501f.
        # Only fields consumed by verify_created are embedded; no ignored RAW dependency.
        captured = {'Image': 'sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e',
         'Name': '/corsa-d1-sg32-4ccb95d2d695ca683fe5174ed789315f-preparer',
         'Config': {'Labels': {'corsa.d1.owner': '78b5228dcf034159a84bd1bd65813fc2',
                               'corsa.loadstand': '',
                               'corsa.sg32.owner': '4ccb95d2d695ca683fe5174ed789315f',
                               'corsa.sg32.role': 'preparer'},
                    'User': '0:0',
                    'Env': None},
         'HostConfig': {'Privileged': False,
                        'ReadonlyRootfs': True,
                        'CapDrop': ['ALL'],
                        'SecurityOpt': ['no-new-privileges'],
                        'RestartPolicy': {'Name': 'no', 'MaximumRetryCount': 0},
                        'CapAdd': ['CAP_CHOWN'],
                        'Memory': 134217728,
                        'MemorySwap': 134217728,
                        'NanoCpus': 250000000,
                        'PidsLimit': 32,
                        'NetworkMode': 'none',
                        'PidMode': '',
                        'CgroupnsMode': 'private',
                        'Binds': None,
                        'PortBindings': {}},
         'Mounts': [{'Type': 'volume',
                     'Destination': '/inputs',
                     'Name': 'corsa-d1-sg32-4ccb95d2d695ca683fe5174ed789315f-inputs',
                     'RW': True},
                    {'Type': 'volume',
                     'Destination': '/work',
                     'Name': 'corsa-d1-sg32-4ccb95d2d695ca683fe5174ed789315f-state',
                     'RW': True},
                    {'Type': 'volume',
                     'Destination': '/run/c1',
                     'Name': 'corsa-d1-sg32-4ccb95d2d695ca683fe5174ed789315f-secret',
                     'RW': True}],
         'State': {'Running': False}}
        h = self.h
        actual_nonce = "4ccb95d2d695ca683fe5174ed789315f"
        actual_volumes = h.volume_names(actual_nonce)
        self.assertEqual(captured["HostConfig"]["CapAdd"], ["CAP_CHOWN"])
        self.assertIsNone(h.verify_created(copy.deepcopy(captured), "preparer", actual_nonce, actual_volumes))
        for case in ("extra", "duplicate", "wrong-case", "other", "service-CHOWN", "service-CAP_CHOWN", "driver-CHOWN", "driver-CAP_CHOWN"):
            if case.startswith(("service-", "driver-")):
                role, capability = case.split("-", 1)
                checked_nonce = NONCE
                volumes = h.volume_names(checked_nonce)
                meta = container_meta(role, volumes)
                meta["HostConfig"]["CapAdd"] = [capability]
            else:
                role, checked_nonce, volumes = "preparer", actual_nonce, actual_volumes
                meta = copy.deepcopy(captured)
                meta["HostConfig"]["CapAdd"] = {"extra": ["CAP_CHOWN", "CAP_SYS_ADMIN"],
                                                  "duplicate": ["CHOWN", "CAP_CHOWN"],
                                                  "wrong-case": ["cap_chown"],
                                                  "other": ["CAP_SYS_ADMIN"]}[case]
            with self.subTest(capability=case), self.assertRaisesRegex(h.Fault, "actual-capabilities"):
                h.verify_created(meta, role, checked_nonce, volumes, "1" * 64)


if __name__ == "__main__":
    unittest.main(verbosity=2)
