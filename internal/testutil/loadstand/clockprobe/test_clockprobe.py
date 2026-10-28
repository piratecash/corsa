"""Pure actual-function controls. No Docker, nsfs, VM or application execution."""
import base64
import contextlib
import hashlib
import copy
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import time
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
NONCE = "a" * 32


def load_subject(name):
    path = HERE / (name + ".py")
    module = types.ModuleType("clock_probe_subject_" + name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    return module


def stat_bytes(pid, birth=111):
    return (str(pid) + " (fixture (actor)) S " + " ".join(["0"] * 18 + [str(birth)]) + "\n").encode("ascii")


def status_bytes(pid, tgid, nspid=None):
    values = [pid] if nspid is None else nspid
    return ("Pid:\t" + str(pid) + "\nTgid:\t" + str(tgid) + "\nNSpid:\t" + " ".join(map(str, values)) + "\nUid:\t501 501 501 501\nGid:\t20 20 20 20\n").encode("ascii")


class CaptureFixture:
    """Controlled proc bytes, real temporary FDs, and an explicit fake clock."""
    def __init__(self, subject, *, mismatch=None, error=None, pid=42, tid=43):
        self.subject, self.pid, self.tid = subject, pid, tid
        self.mismatch, self.error = mismatch, error
        self.folder = tempfile.TemporaryDirectory(prefix="clockprobe-pure-")
        self.files = [Path(self.folder.name) / n for n in ("namespace-a", "namespace-b")]
        for path in self.files:
            path.write_bytes(b"fixture, not nsfs\n")
        self.alive, self.first_fds, self.opens, self.reads = set(), set(), {}, {}
        self.paths, self.closed = [], []
        self.clock_values = iter((200, 100, 300) if error == "clock-regression" else (100, 150, 200, 300, 350, 400))
        self.stack = None
        self.close_fault_used = False

    def __enter__(self):
        import contextlib
        self.stack = contextlib.ExitStack()
        original_os = types.SimpleNamespace(**vars(os))
        original_os.open, original_os.close, original_os.readlink = self.open, self.close, self.readlink
        original_os.getpid = lambda: self.pid
        self.stack.enter_context(mock.patch.object(self.subject, "os", original_os))
        self.stack.enter_context(mock.patch.object(self.subject, "threading", types.SimpleNamespace(get_native_id=lambda: self.tid)))
        self.stack.enter_context(mock.patch.object(self.subject, "read_file", self.read_file))
        self.stack.enter_context(mock.patch.object(self.subject, "boottime", lambda: next(self.clock_values)))
        return self

    def __exit__(self, *args):
        self.stack.close()
        for fd in tuple(self.alive):
            os.close(fd)
            self.alive.remove(fd)
        self.folder.cleanup()

    def open(self, path, flags):
        self.paths.append(path)
        self.opens[path] = self.opens.get(path, 0) + 1
        if not path.startswith("/proc/") or "/ns/" not in path:
            raise AssertionError("unexpected actual capture path: " + str(path))
        index = 0
        if self.mismatch == "children" and path.endswith("/time_for_children"):
            index = 1
        if self.mismatch == "numeric" and path.startswith("/proc/" + str(self.pid) + "/ns/"):
            index = 1
        if self.mismatch == "target" and path.startswith("/proc/1/ns/") and self.pid != 1:
            index = 1
        if self.mismatch == "actor" and any(path.startswith(prefix + "/ns/") for prefix in ("/proc/self", "/proc/thread-self", "/proc/" + str(self.pid), "/proc/" + str(100 if self.mismatch == "pid-view" else self.tid))):
            index = 1
        if self.error == "named-change" and path == "/proc/self/ns/time" and self.opens[path] > 1:
            index = 1
        fd = os.open(self.files[index], flags)
        self.alive.add(fd)
        if self.opens[path] == 1:
            self.first_fds.add(fd)
        return fd

    def close(self, fd):
        os.close(fd)
        self.alive.remove(fd)
        self.closed.append(fd)
        if self.error == "close" and fd in self.first_fds and not self.close_fault_used:
            self.close_fault_used = True
            raise OSError("controlled held close failure after actual close")

    def readlink(self, path):
        # /proc/thread-self is a task directory: Linux exposes offsets through
        # its proc-visible top-level TID, not a timens_offsets task child.
        if path == "/proc/thread-self":
            return "99/task/100" if self.mismatch == "pid-view" else str(self.pid) + "/task/" + str(self.tid)
        # Record only; no claim that these regular files are actual namespaces.
        index = 0
        if self.mismatch == "children" and path.endswith("/time_for_children"):
            index = 1
        if self.mismatch == "numeric" and path.startswith("/proc/" + str(self.pid) + "/ns/"):
            index = 1
        if self.mismatch == "target" and path.startswith("/proc/1/ns/") and self.pid != 1:
            index = 1
        if self.mismatch == "actor" and any(path.startswith(prefix + "/ns/") for prefix in ("/proc/self", "/proc/thread-self", "/proc/" + str(self.pid), "/proc/" + str(100 if self.mismatch == "pid-view" else self.tid))):
            index = 1
        return "time:[" + str(os.stat(self.files[index]).st_ino) + "]"

    def read_file(self, path, limit):
        self.paths.append(path)
        self.reads[path] = self.reads.get(path, 0) + 1
        if path == "/proc/thread-self/timens_offsets":
            raise FileNotFoundError(2, "task directory has no timens_offsets", path)
        if self.error == "read" and path == "/proc/self/timens_offsets":
            raise OSError("controlled offset read failure")
        if path == "/proc/self/mountinfo":
            raw = b"36 25 0:34 / /proc rw,nosuid,nodev,noexec - proc proc rw\n"
            if self.error == "mount":
                raw = b"36 25 0:34 / /proc rw - \n"
        elif path.endswith("/timens_offsets"):
            raw = b"monotonic -7 123\nboottime 8 456\n"
            if self.error == "offset":
                raw = b"monotonic 0 0\n"
        else:
            prefix, leaf = path.rsplit("/", 1)
            if prefix == "/proc/self":
                pid, tgid, nspid = self.pid, self.pid, [self.pid]
                if self.mismatch == "pid-view":
                    pid, tgid, nspid = 99, 99, [99, self.pid]
            elif prefix == "/proc/thread-self":
                pid, tgid, nspid = self.tid, self.pid, [self.tid]
                if self.mismatch == "pid-view":
                    pid, tgid, nspid = 100, 99, [100, self.tid]
            elif prefix == "/proc/" + str(self.pid):
                pid, tgid, nspid = self.pid, self.pid, [self.pid]
            elif prefix == "/proc/" + str(100 if self.mismatch == "pid-view" else self.tid):
                pid = 100 if self.mismatch == "pid-view" else self.tid
                tgid = 99 if self.mismatch == "pid-view" else self.pid
                nspid = [pid]
                if self.mismatch == "pid-view":
                    nspid = [100, self.tid]
            elif prefix == "/proc/1":
                pid, tgid, nspid = 1, 1, [1]
            else:
                raise AssertionError("unexpected proc prefix " + prefix)
            if leaf == "stat":
                birth = 222 if self.error == "birth" and path == "/proc/self/stat" and self.reads[path] > 1 else 111
                raw = stat_bytes(pid, birth)
            elif leaf == "status":
                raw = status_bytes(pid, tgid, nspid)
            else:
                raise AssertionError("unexpected proc leaf " + leaf)
        if len(raw) > limit:
            raise AssertionError("fixture exceeds real fixed input budget")
        return raw


def actor_packets(probe, observer_mismatch=None):
    records, observed = [], []
    with CaptureFixture(probe, pid=1, tid=1) as keeper:
        keeper.clock_values = iter((100, 150, 200, 700, 750, 800))
        def stop(_):
            with CaptureFixture(probe, mismatch=observer_mismatch) as observer:
                observer.files = keeper.files
                observer.clock_values = iter((300, 350, 400))
                observed.append(probe.capture("observer", "observer", NONCE))
            return b"STOP\n"
        fake_sys = types.SimpleNamespace(stdin=types.SimpleNamespace(buffer=types.SimpleNamespace(readline=stop)))
        with mock.patch.object(probe, "sys", fake_sys), mock.patch.object(probe, "emit", lambda v: records.append(copy.deepcopy(v))):
            if probe.main(["keeper", NONCE]) != 0:
                raise AssertionError("controlled actor fixture did not complete")
    return records[0], observed[0], records[1]


class ThreadOffsetFixture(CaptureFixture):
    """Causal top-level TID source; the task-directory file never exists."""
    def __init__(self, subject, *, issue=None, link_value=None, **kwargs):
        super().__init__(subject, **kwargs)
        self.issue, self.link_value = issue, link_value
        self.thread_link_reads = 0
        self.reader_fd = None
        self.reader_close_fault_used = False
        self.distinct_offsets = b"monotonic -19 123\nboottime 23 456\n"

    @property
    def reader(self):
        return "/proc/" + str(100 if self.mismatch == "pid-view" else self.tid)

    def readlink(self, path):
        if path != "/proc/thread-self":
            return super().readlink(path)
        self.thread_link_reads += 1
        if self.issue == "link-read":
            raise OSError("controlled thread link unavailable")
        if self.issue == "link-transition" and self.thread_link_reads > 1:
            return str(self.pid) + "/task/" + str(self.tid + 1)
        return self.link_value if self.link_value is not None else super().readlink(path)

    def read_file(self, path, limit):
        if path == self.reader + "/timens_offsets" and self.issue == "reader-absent":
            raise FileNotFoundError(2, "controlled top-level reader missing", path)
        raw = super().read_file(path, limit)
        if path == self.reader + "/stat":
            if self.issue == "reader-pid":
                raw = stat_bytes(self.tid + 1)
            elif self.issue == "birth-transition" and self.reads[path] > 1:
                raw = stat_bytes(self.tid, 222)
        elif path == self.reader + "/status":
            if self.issue == "reader-tgid":
                raw = status_bytes(self.tid, self.pid + 1)
            elif self.issue == "status-transition" and self.reads[path] > 1:
                raw = status_bytes(self.tid, self.pid, [self.tid, self.tid])
        elif path == self.reader + "/timens_offsets":
            if self.issue == "distinct-offsets":
                raw = self.distinct_offsets
            elif self.issue == "offset-transition" and self.reads[path] > 1:
                raw = self.distinct_offsets
        if len(raw) > limit:
            raise AssertionError("controlled top-level reader exceeds input bound")
        return raw

    def open(self, path, flags):
        fd = super().open(path, flags)
        if path == self.reader + "/ns/time" and self.opens[path] == 1:
            self.reader_fd = fd
        changed_kind = "time_for_children" if self.issue == "children-namespace-transition" else "time"
        if (self.issue in ("namespace-transition", "children-namespace-transition")
                and path == self.reader + "/ns/" + changed_kind and self.opens[path] > 1):
            self.close(fd)
            fd = os.open(self.files[1], flags)
            self.alive.add(fd)
        return fd

    def close(self, fd):
        super().close(fd)
        if self.issue == "reader-close" and fd == self.reader_fd and not self.reader_close_fault_used:
            self.reader_close_fault_used = True
            raise OSError("controlled reader held-FD close failure after actual close")


class HoldObserved(BaseException):
    pass


class RunFixture:
    """Actual host workflow, fake Docker/process/time boundary, owned files."""
    def __init__(self, host, packets, *, failure=None):
        self.h, self.packets, self.failure = host, copy.deepcopy(packets), failure
        self.folder = tempfile.TemporaryDirectory(prefix="clockprobe-host-pure-")
        self.parent = Path(self.folder.name).resolve(strict=True)
        self.runs = self.parent / "runs"
        self.runs.mkdir(mode=0o700)
        self.admission = self.parent / "admission.json"
        self.calls, self.removed, self.owned, self.children = [], [], {}, []
        self.holds, self.now, self.nonce_count = 0, 100., 0
        self.stdout = io.StringIO()
        self.source = (HERE / "probe.py").read_bytes()
        self.host_source = (HERE / "host.py").read_bytes()
        self.wrapper = b"controlled wrapper metadata only\n"
        self.cfg = {"owner_approved": True, "one_attempt": True, "allow_recovery_hold": True,
            "profile": "d1-clock-probe-v1", "run_parent": str(self.runs),
            "probe_sha256": hashlib.sha256(self.source).hexdigest(),
            "host_sha256": hashlib.sha256(self.host_source).hexdigest(),
            "wrapper_sha256": hashlib.sha256(self.wrapper).hexdigest()}

    def __enter__(self):
        h = self.h
        self.stack = contextlib.ExitStack()
        fake_os = types.SimpleNamespace(**vars(os))
        fake_os.getuid, fake_os.getgid, fake_os.getpgrp = lambda: 501, lambda: 20, lambda: 77
        fake_os.statvfs = lambda _: types.SimpleNamespace(f_bavail=32 * 1024**3, f_frsize=1)
        fake_time = types.SimpleNamespace(**vars(time))
        fake_time.monotonic, fake_time.sleep = lambda: self.now, self.hold
        self.stack.enter_context(mock.patch.object(h, "os", fake_os))
        self.stack.enter_context(mock.patch.object(h, "time", fake_time))
        self.stack.enter_context(mock.patch.object(h, "read_owned", self.read_owned))
        self.stack.enter_context(mock.patch.object(h, "command", self.command))
        self.stack.enter_context(mock.patch.object(h, "Child", self.child))
        self.stack.enter_context(mock.patch.object(h.secrets, "token_hex", self.nonce))
        self.original_put = h.put
        self.stack.enter_context(mock.patch.object(h, "put", self.put))
        self.stack.enter_context(contextlib.redirect_stdout(self.stdout))
        return self

    def __exit__(self, *args):
        self.stack.close()
        self.folder.cleanup()

    def nonce(self, size):
        if size != 16:
            raise AssertionError("nonce budget changed")
        self.nonce_count += 1
        return NONCE if self.nonce_count == 1 else "b" * 32

    def hold(self, seconds):
        if seconds != 3600:
            raise AssertionError("unexpected actual sleep")
        self.holds += 1
        raise HoldObserved()

    def read_owned(self, path, limit):
        path = Path(path)
        if path == self.admission:
            raw = json.dumps(self.cfg).encode()
        elif path == HERE / "probe.py":
            raw = self.source
        elif path == HERE / "host.py":
            raw = self.host_source
        elif path == self.h.WRAPPER:
            raw = self.wrapper
        elif path == self.h.LOCK / "owner":
            raw = b"name=d1-clock-probe\npgid=77\n"
        else:
            raise AssertionError("unexpected host read " + str(path))
        if len(raw) > limit:
            raise AssertionError("fixture input exceeds actual bound")
        return raw

    def put(self, path, value):
        if self.failure in ("receipt", "cleanup-and-receipt") and Path(path).name == "result.json":
            raise OSError("controlled final receipt failure")
        return self.original_put(path, value)

    def metadata(self, ident):
        role = self.owned[ident]
        return {"Id": ident, "Image": self.h.IMAGE, "User": "501:20", "Mounts": [],
            "Privileged": False, "ReadonlyRootfs": True, "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"], "Memory": self.h.MEMORY,
            "MemorySwap": self.h.MEMORY, "NanoCpus": 250000000, "PidsLimit": 32,
            "Labels": {"corsa.clockprobe.owner": NONCE, "corsa.clockprobe.role": role},
            "Name": "/corsa-d1-clockprobe-" + NONCE + "-" + role,
            "PidMode": "" if role == "keeper" else "container:" + "1" * 64,
            "NetworkMode": "none" if role == "keeper" else "container:" + "1" * 64,
            "State": {"Running": False, "ExitCode": 0, "OOMKilled": False}}

    def command(self, args, deadline, permit_failure=False):
        args = list(args)
        self.calls.append((args, deadline, permit_failure))
        if self.now >= deadline:
            raise self.h.Fault("controlled deadline")
        if args[0] == "context":
            raw = json.dumps("unix:///Users/hades/.docker/run/docker.sock").encode()
        elif args[0] == "info":
            raw = b'"controlled-server"'
            if self.failure == "preflight-deadline":
                self.now += 31
        elif args[:2] == ["image", "inspect"]:
            raw = json.dumps({"Id": self.h.IMAGE, "Os": "linux", "Architecture": "arm64", "Volumes": None}).encode()
        elif args[0] == "create":
            role = args[-2]
            ident = ("1" if role == "keeper" else "2") * 64
            self.owned[ident] = role
            raw = ident.encode() + b"\n"
        elif args[0] == "inspect":
            ident = args[-1]
            if '\"Owner\":' in args[2]:
                role = self.owned.get(ident, "keeper")
                raw = json.dumps({"Owner": "wrong" if ident == "f" * 64 else NONCE,
                    "Role": role, "Name": "/corsa-d1-clockprobe-" + NONCE + "-" + role}).encode()
            else:
                raw = json.dumps(self.metadata(ident)).encode()
        elif args[0] == "ps":
            if self.failure == "cleanup-and-receipt":
                raise self.h.Fault("controlled cleanup query failure")
            id_filter = next((arg[3:] for arg in args if arg.startswith("id=")), None)
            ids = ([id_filter] if id_filter in self.owned else []) if id_filter else list(self.owned)
            if self.failure == "foreign" and id_filter is None:
                ids.insert(0, "f" * 64)
            raw = "\n".join(ids).encode()
        elif args[:2] == ["rm", "-f"]:
            ident = args[-1]
            if ident not in self.owned:
                raise AssertionError("attempted foreign mutation")
            self.removed.append(ident)
            del self.owned[ident]
            raw = ident.encode() + b"\n"
        else:
            raise AssertionError("unexpected Docker command " + repr(args))
        return raw, 0

    def child(self, args, label, interactive=False):
        fixture, h = self, self.h
        if args[0] != "start" or label not in ("keeper", "observer"):
            raise AssertionError("unexpected child boundary")
        class Sink:
            data, closed = b"", False
            def write(self, raw):
                self.data += raw
                return len(raw)
            def flush(self):
                pass
            def close(self):
                self.closed = True
        class Attached:
            def __init__(self):
                self.out, self.err = bytearray(), bytearray()
                self.p = types.SimpleNamespace(returncode=None, stdin=Sink() if interactive else None)
                h.LIVE.append(self)
                fixture.children.append(self)
            def pump(self, deadline, first_line=False):
                if fixture.now >= deadline:
                    raise h.Fault("controlled attach deadline")
                if label == "observer":
                    values = [fixture.packets[1]]
                    self.p.returncode = 0
                elif self.p.stdin.closed:
                    if self.p.stdin.data != b"STOP\n":
                        raise AssertionError("STOP protocol changed")
                    values = [fixture.packets[0], fixture.packets[2]]
                    self.p.returncode = 0
                else:
                    values = [fixture.packets[0]]
                self.out = bytearray(b"".join(json.dumps(v, separators=(",", ":")).encode() + b"\n" for v in values))
            def close(self, deadline):
                self.p.returncode = 0
                return True
        return Attached()


class VirtualStream:
    def __init__(self, fd):
        self.fd, self.closed = fd, False
    def fileno(self):
        return self.fd
    def close(self):
        self.closed = True


class VirtualSelector:
    def __init__(self, fail=False):
        self.rows, self.closed, self.fail = {}, False, fail
    def register(self, stream, events, data):
        if self.fail:
            raise OSError("controlled selector setup failure")
        self.rows[stream.fd] = types.SimpleNamespace(fd=stream.fd, fileobj=stream, data=data)
    def get_map(self):
        return self.rows
    def select(self, timeout):
        return [(key, 1) for key in list(self.rows.values())]
    def unregister(self, stream):
        del self.rows[stream.fd]
    def close(self):
        self.closed = True
        self.rows.clear()


class VirtualProcess:
    def __init__(self, wait_failure=False):
        self.stdin = None
        self.stdout, self.stderr = VirtualStream(100), VirtualStream(101)
        self.returncode, self.wait_failure = None, wait_failure
        self.events = []
    def poll(self):
        return self.returncode
    def terminate(self):
        self.events.append("terminate")
    def kill(self):
        self.events.append("kill")
    def wait(self, timeout):
        self.events.append(("wait", timeout))
        if self.wait_failure:
            raise subprocess.TimeoutExpired("controlled-no-process", timeout)
        self.returncode = 0
        return 0


class ClockProbePureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.probe = load_subject("probe")
        cls.host = load_subject("host")

    def test_01_real_fd_identity_and_bounded_reader(self):
        p = self.probe
        with tempfile.TemporaryDirectory(prefix="clockprobe-reader-") as directory:
            path = Path(directory) / "input"
            path.write_bytes(b"four")
            self.assertEqual(p.read_file(str(path), 4), b"four")
            with self.assertRaises(p.Fault):
                p.read_file(str(path), 3)
            fd = os.open(path, os.O_RDONLY)
            other = os.open(path, os.O_RDONLY)
            try:
                self.assertEqual(p.ns_identity(fd), p.ns_identity(other))
                before = p.ns_identity(fd)
                path.unlink()
                path.write_bytes(b"replacement")
                new = os.open(path, os.O_RDONLY)
                try:
                    self.assertEqual(p.ns_identity(fd), before)
                    self.assertNotEqual(p.ns_identity(new), before)
                finally:
                    os.close(new)
            finally:
                os.close(other)
                os.close(fd)

    def test_02_parser_and_boottime_strict_refusals(self):
        p = self.probe
        self.assertEqual(p.proc_stat(stat_bytes(42)), {"pid": 42, "ppid": 0, "start_ticks": 111})
        self.assertEqual(p.proc_status(status_bytes(42, 42))["NSpid"], [42])
        value = p.parse_offsets(b"monotonic -7 123\nboottime 8 456\n")
        self.assertEqual(value["monotonic"], {"seconds": -7, "nanoseconds": 123})
        for raw in (b"", b"monotonic 0 0\n", b"monotonic 0 1000000000\nboottime 0 0\n", b"monotonic 0 0\nmonotonic 0 0\nboottime 0 0\n"):
            with self.subTest(offset=raw), self.assertRaises(p.Fault):
                p.parse_offsets(raw)
        with self.assertRaises(p.Fault):
            p.proc_status(status_bytes(42, 42) + b"Pid: 42\n")
        with self.assertRaises(p.Fault):
            p.proc_stat(b"incomplete")
        with self.assertRaises(p.Fault):
            p.proc_mounts(b"36 25 0:34 / /proc rw - \n")
        for value in (True, -1, 1 << 63):
            clock = types.SimpleNamespace(CLOCK_BOOTTIME=7, clock_gettime_ns=lambda _, v=value: v)
            with mock.patch.object(p, "time", clock), self.assertRaises(p.Fault):
                p.boottime()
        with mock.patch.object(p, "time", types.SimpleNamespace()), self.assertRaises(p.Fault):
            p.boottime()
        with mock.patch.object(p, "time", types.SimpleNamespace(CLOCK_BOOTTIME=7, clock_gettime_ns=lambda _: 123)):
            self.assertEqual(p.boottime(), 123)

    def test_03_actual_capture_paths_and_complete_matrix(self):
        p = self.probe
        with CaptureFixture(p) as fixture:
            packet = p.capture("observer", "observer", NONCE)
            self.assertTrue(packet["complete"])
            self.assertTrue(packet["supported_source_guard"])
            self.assertTrue(packet["shared_time_domain"])
            self.assertTrue(packet["actor_current_equals_target_current"])
            self.assertTrue(packet["old_numeric_comparator_equal"])
            self.assertEqual(packet["qualification"], "UNKNOWN")
            self.assertEqual(packet["own_boottime_bracket_ns"], [100, 200])
            self.assertEqual(set(packet["contexts"]), {"self", "thread_self", "numeric_pid", "target_keeper"})
            for prefix in ("/proc/self", "/proc/thread-self", "/proc/42", "/proc/1"):
                self.assertIn(prefix + "/stat", fixture.paths)
                self.assertIn(prefix + "/status", fixture.paths)
                if prefix != "/proc/thread-self":
                    self.assertIn(prefix + "/timens_offsets", fixture.paths)
                self.assertGreaterEqual(fixture.opens[prefix + "/ns/time"], 2)
                self.assertGreaterEqual(fixture.opens[prefix + "/ns/time_for_children"], 2)
            self.assertNotIn("/proc/thread-self/timens_offsets", fixture.paths)
            self.assertEqual(fixture.reads["/proc/43/timens_offsets"], 2)
            offsets = packet["contexts"]["self"]["offsets_for_children"]
            self.assertEqual(base64.b64decode(offsets["raw_base64"]), b"monotonic -7 123\nboottime 8 456\n")
            self.assertEqual(offsets["values"]["boottime"]["seconds"], 8)
            self.assertEqual(fixture.alive, set())

    def test_04_stable_mismatches_are_complete_but_guard_false(self):
        p = self.probe
        for mismatch in ("children", "numeric", "target", "pid-view"):
            with self.subTest(mismatch=mismatch), CaptureFixture(p, mismatch=mismatch) as fixture:
                packet = p.capture("observer", "observer", NONCE)
                self.assertTrue(packet["complete"], packet)
                self.assertFalse(packet["supported_source_guard"])
                self.assertFalse(packet["shared_time_domain"])
                self.assertIsInstance(packet["source_refusal"], str)
                self.assertEqual(packet["qualification"], "UNKNOWN")
                self.assertEqual(packet["own_boottime_bracket_ns"], [100, 200])
                if mismatch in ("children", "numeric", "pid-view"):
                    self.assertTrue(packet["actor_current_equals_target_current"])
                if mismatch == "pid-view":
                    self.assertTrue(packet["old_numeric_comparator_equal"])
                    self.assertFalse(packet["actor_pid_view_matches"])
                if mismatch == "numeric":
                    self.assertFalse(packet["old_numeric_comparator_equal"])
                if mismatch == "children":
                    self.assertFalse(packet["current_equals_for_children"]["self"])
                    self.assertEqual(packet["contexts"]["self"]["offsets_for_children"]["values"]["boottime"]["seconds"], 8)
                self.assertEqual(fixture.alive, set())

    def test_05_changed_missing_or_failed_context_is_unknown(self):
        p = self.probe
        for error in ("named-change", "birth", "read", "offset", "mount", "clock-regression", "close"):
            with self.subTest(error=error), CaptureFixture(p, error=error) as fixture:
                packet = p.capture("observer", "observer", NONCE)
                self.assertFalse(packet["complete"])
                self.assertIsNone(packet["supported_source_guard"])
                self.assertIsNone(packet["shared_time_domain"])
                self.assertTrue(packet["errors"])
                self.assertEqual(packet["qualification"], "UNKNOWN")
                self.assertEqual(fixture.alive, set())

    def test_06_emission_and_main_lifetime_bounds(self):
        p = self.probe
        buffer = io.BytesIO()
        with mock.patch.object(p, "sys", types.SimpleNamespace(stdout=types.SimpleNamespace(buffer=buffer))):
            p.emit({"complete": True})
            self.assertTrue(buffer.getvalue().endswith(b"\n"))
            with self.assertRaises(p.Fault):
                p.emit({"raw": "x" * p.ROLE_MAX})
        records = []
        with CaptureFixture(p, pid=1, tid=1) as fixture:
            def stop(limit):
                self.assertEqual(limit, 6)
                self.assertGreaterEqual(len(fixture.alive), 6)
                return b"STOP\n"
            fake_sys = types.SimpleNamespace(stdin=types.SimpleNamespace(buffer=types.SimpleNamespace(readline=stop)))
            with mock.patch.object(p, "sys", fake_sys), mock.patch.object(p, "emit", lambda value: records.append(copy.deepcopy(value))):
                self.assertEqual(p.main(["keeper", NONCE]), 0)
            self.assertEqual([row["phase"] for row in records], ["keeper-ready", "keeper-stop"])
            self.assertEqual(records[0]["lifetime_held_before"], records[1]["lifetime_held_after"])
            self.assertEqual(fixture.alive, set())
        with mock.patch.object(p, "capture", return_value={"complete": False}), mock.patch.object(p, "emit"):
            self.assertEqual(p.main(["observer", NONCE]), 2)
        with self.assertRaises(p.Fault):
            p.main(["observer", "invalid"])

    def test_07_host_strict_packets_and_explicit_profile(self):
        p, h = self.probe, self.host
        with CaptureFixture(p):
            good = p.capture("observer", "observer", NONCE)
        raw = json.dumps(good).encode() + b"\n"
        self.assertEqual(h.packet(raw, "observer", "observer", NONCE), good)
        mutations = []
        for key, value in (("pid", True), ("native_tid", True), ("complete", 1),
                           ("supported_source_guard", None), ("contexts", {}),
                           ("own_boottime_bracket_ns", [200, 100])):
            bad = copy.deepcopy(good)
            bad[key] = value
            mutations.append(bad)
        bad = copy.deepcopy(good)
        bad["contexts"]["self"]["time"][0] = True
        mutations.append(bad)
        for index, bad in enumerate(mutations):
            with self.subTest(invalid=index), self.assertRaises((h.Fault, ValueError)):
                h.packet(json.dumps(bad).encode() + b"\n", "observer", "observer", NONCE)
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}'):
            with self.assertRaises(h.Fault):
                h.strict_json(raw)
        with self.assertRaises((h.Fault, ValueError)):
            h.packet(b"x" * (h.MAX_ROLE + 1) + b"\n", "observer", "observer", NONCE)
        with mock.patch.object(h, "PROBE_TEXT", "controlled-producer-code"):
            for role in ("keeper", "observer"):
                args = h.create_argv(role, NONCE, "1" * 64 if role == "observer" else None)
                opts = args[:args.index(h.IMAGE)]
                for flag in ("--pull=never", "--cpus=0.25", "--memory=134217728", "--memory-swap=134217728", "--pids-limit=32", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges"):
                    self.assertIn(flag, opts)
                self.assertEqual(args[-9:], ["--entrypoint=/usr/bin/python3", h.IMAGE, "-I", "-S", "-B", "-c", "controlled-producer-code", role, NONCE])
                self.assertFalse(any(v.startswith(("--mount", "--volume", "--tmpfs", "--privileged", "--device", "--env")) for v in opts))
                if role == "keeper":
                    self.assertIn("--network=none", opts)
                else:
                    self.assertIn("--pid=container:" + "1" * 64, opts)
                    self.assertIn("--network=container:" + "1" * 64, opts)
        with self.assertRaises(h.Fault):
            h.create_argv("observer", NONCE, "wrong")

    def test_08_cross_actor_join_and_custodied_one_claim(self):
        p, h = self.probe, self.host
        packets = actor_packets(p)
        joined = h.join_packets(*packets)
        self.assertTrue(joined["cross_actor_binding_equal"])
        self.assertTrue(joined["actor_current_equal"])
        self.assertTrue(joined["shared_time_domain"])
        for kind in ("birth", "namespace", "lifetime", "keeper-pid"):
            changed = copy.deepcopy(packets)
            if kind == "birth":
                changed[1]["contexts"]["target_keeper"]["process"]["start_ticks"] += 1
            elif kind == "namespace":
                changed[1]["contexts"]["target_keeper"]["time"][1] += 1
            elif kind == "lifetime":
                changed[2]["lifetime_held_after"]["self"]["time"][1] += 1
            else:
                changed[0]["pid"] = 2
            with self.subTest(join=kind):
                self.assertFalse(h.join_packets(*changed)["shared_time_domain"])
        with tempfile.TemporaryDirectory(prefix="clockprobe-custody-") as directory:
            path = Path(directory) / "owned.json"
            h.put(path, {"one": True})
            self.assertEqual(h.strict_json(h.read_owned(path, 128)), {"one": True})
            before = path.read_bytes()
            with self.assertRaises(OSError):
                h.put(path, {"two": True})
            self.assertEqual(path.read_bytes(), before)
            link = Path(directory) / "alias"
            link.symlink_to(path)
            with self.assertRaises((OSError, h.Fault)):
                h.read_owned(link, 128)
        with RunFixture(h, packets) as fixture:
            self.assertEqual(h.run(fixture.admission), 0)
            self.assertEqual(fixture.owned, {})
            self.assertEqual(set(fixture.removed), {"1" * 64, "2" * 64})
            receipts = list(fixture.runs.glob("*/result.json"))
            self.assertEqual(len(receipts), 1)
            result = json.loads(receipts[0].read_bytes())
            self.assertTrue(result["diagnostic_complete"])
            self.assertFalse(result["resource_qualification"])
            creates = sum(args[0] == "create" for args, _, _ in fixture.calls)
            with self.assertRaises(OSError):
                h.run(fixture.admission)
            self.assertEqual(sum(args[0] == "create" for args, _, _ in fixture.calls), creates)

        # Stable actor-current difference with the observer's keeper view still
        # bound to the keeper: complete diagnosis is not source-guard support.
        mismatch_packets = actor_packets(p, observer_mismatch="actor")
        with RunFixture(h, mismatch_packets) as fixture:
            self.assertEqual(h.run(fixture.admission), 0)
            result = json.loads(next(fixture.runs.glob("*/result.json")).read_bytes())
            self.assertTrue(result["diagnostic_complete"])
            self.assertTrue(result["cross_actor_binding_equal"])
            self.assertFalse(result["actor_current_equal"])
            self.assertFalse(result["shared_time_domain"])
            self.assertFalse(result["resource_qualification"])
            self.assertEqual(fixture.owned, {})

    def test_09_host_failure_deadline_and_live_hold_survive_receipt_errors(self):
        h = self.host
        packets = actor_packets(self.probe)
        with RunFixture(h, packets, failure="receipt") as fixture:
            self.assertEqual(h.run(fixture.admission), 2)
            self.assertEqual(fixture.owned, {})
            self.assertEqual(fixture.holds, 0)
        for failure in ("cleanup-and-receipt", "foreign"):
            with self.subTest(failure=failure), RunFixture(h, packets, failure=failure) as fixture:
                with self.assertRaises(HoldObserved):
                    h.run(fixture.admission)
                self.assertEqual(fixture.holds, 1)
                self.assertNotIn("f" * 64, fixture.removed)
                if failure == "cleanup-and-receipt":
                    self.assertTrue(fixture.owned)
        with RunFixture(h, packets, failure="preflight-deadline") as fixture:
            with self.assertRaises(h.Fault):
                h.run(fixture.admission)
            self.assertFalse(fixture.owned)
            self.assertFalse(any(args[0] == "create" for args, _, _ in fixture.calls))
            self.assertEqual(fixture.holds, 0)
        # Owned noncanonical alias is a refusal case, not a host-guard bypass.
        with RunFixture(h, packets) as fixture:
            alias = fixture.parent / "run-parent-alias"
            alias.symlink_to(fixture.runs, target_is_directory=True)
            fixture.cfg["run_parent"] = str(alias)
            with self.assertRaisesRegex(h.Fault, "output-parent-path"):
                h.run(fixture.admission)
            self.assertEqual(fixture.calls, [])
            self.assertEqual(fixture.children, [])
            self.assertFalse((fixture.parent / "ONE-ATTEMPT-CLAIM.json").exists())
        with RunFixture(h, packets) as fixture:
            fixture.cfg["owner_approved"] = False
            with self.assertRaises(h.Fault):
                h.run(fixture.admission)
            self.assertEqual(fixture.calls, [])
            self.assertEqual(fixture.children, [])

    def test_10_actual_child_output_deadline_and_constructor_cleanup(self):
        h = self.host
        for case in ("exact", "one-over", "deadline", "unreaped", "setup-failure"):
            with self.subTest(child=case):
                proc = VirtualProcess(wait_failure=case == "unreaped")
                selector = VirtualSelector(fail=case == "setup-failure")
                remaining = {100: [b"x" * 4096] * 16 + ([b"x"] if case == "one-over" else []) + [b""], 101: [b""]}
                fake_os = types.SimpleNamespace(**vars(os))
                fake_os.set_blocking = lambda *_: None
                def read(fd, size):
                    chunk = remaining[fd].pop(0)
                    if len(chunk) > size:
                        raise AssertionError("controlled read violates syscall size")
                    return chunk
                fake_os.read = read
                fake_process = types.SimpleNamespace(**vars(subprocess))
                def popen(argv, **kwargs):
                    self.assertEqual(argv[:3], [h.DOCKER, "--host", h.ENDPOINT])
                    self.assertFalse(kwargs["start_new_session"])
                    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG"):
                        self.assertNotIn(name, kwargs["env"])
                    return proc
                fake_process.Popen = popen
                h.ROOT, h.TOTAL, h.LIVE = None, 0, []
                with mock.patch.object(h, "os", fake_os), mock.patch.object(h, "subprocess", fake_process), mock.patch.object(h, "selectors", types.SimpleNamespace(DefaultSelector=lambda: selector, EVENT_READ=1)), mock.patch.object(h, "time", types.SimpleNamespace(monotonic=lambda: 100.)):
                    if case == "setup-failure":
                        with self.assertRaises(OSError):
                            h.Child(["fixture"], "fake")
                        self.assertEqual(proc.returncode, 0)
                        self.assertTrue(proc.stdout.closed and proc.stderr.closed)
                        self.assertEqual(len(h.LIVE), 1)
                        continue
                    child = h.Child(["fixture"], "fake")
                    if case in ("one-over", "deadline"):
                        with self.assertRaises(h.Fault):
                            child.pump(99. if case == "deadline" else 101.)
                    elif case != "unreaped":
                        child.pump(101.)
                        self.assertEqual(len(child.out), h.MAX_ROLE)
                    self.assertEqual(child.close(101.), case != "unreaped")
                    self.assertTrue(proc.stdout.closed and proc.stderr.closed)
                    if case == "unreaped":
                        self.assertIn("kill", proc.events)
                        self.assertIsNone(proc.returncode)

        # A failed pump must not reuse a RAW filename during later cleanup.
        # Actual command() logic and O_EXCL files; no actual process is created.
        command_host = load_subject("host")
        labels = []
        with tempfile.TemporaryDirectory(prefix="clockprobe-command-raw-") as directory:
            raw_root = Path(directory)
            class CommandChild:
                def __init__(self, args, label, interactive=False):
                    labels.append(label)
                    self.label = label
                    self.files = [(raw_root / (label + "." + name)).open("xb") for name in ("out", "err")]
                    self.p = types.SimpleNamespace(returncode=0)
                    self.out, self.err = bytearray(), bytearray()
                def pump(self, deadline, first_line=False):
                    if len(labels) == 1:
                        self.files[0].write(b"retained partial RAW")
                        self.out.extend(b"retained partial RAW")
                        raise command_host.Fault("controlled pump failure before CALLS append")
                    self.out.extend(b"[]")
                    self.files[0].write(b"[]")
                def close(self, deadline):
                    for stream in self.files:
                        stream.flush()
                        stream.close()
                    return True
            with mock.patch.object(command_host, "Child", CommandChild):
                with self.assertRaises(command_host.Fault):
                    command_host.command(["fixture-failed-query"], 101.)
                out, code = command_host.command(["fixture-cleanup-query"], 101.)
            self.assertEqual((out, code), (b"[]", 0))
            self.assertEqual(len(labels), 2)
            self.assertNotEqual(labels[0], labels[1])
            self.assertEqual((raw_root / (labels[0] + ".out")).read_bytes(), b"retained partial RAW")
            self.assertEqual((raw_root / (labels[1] + ".out")).read_bytes(), b"[]")
            self.assertEqual(len(command_host.CALLS), 2)
            self.assertEqual([row["attempt"] for row in command_host.CALLS], [0, 1])
            self.assertIsNone(command_host.CALLS[0]["out"])
            self.assertEqual(command_host.CALLS[0]["out_bytes"], len(b"retained partial RAW"))


    def test_11_actual_thread_offsets_reader_uses_proc_visible_tid(self):
        p, h = self.probe, self.host
        with ThreadOffsetFixture(p) as fixture:
            packet = p.capture("observer", "observer", NONCE)
            self.assertTrue(packet["complete"], packet)
            row = packet["contexts"]["thread_self"]
            proof = row["offsets_reader"]
            self.assertEqual(proof["path"], "/proc/43")
            self.assertEqual(proof["thread_self_link_before"], "42/task/43")
            self.assertEqual(proof["thread_self_link_after"], "42/task/43")
            self.assertEqual(row["process"]["pid"], 43)
            self.assertEqual(row["status"]["Tgid"], [42])
            self.assertEqual(proof["before"], proof["after"])
            self.assertEqual(fixture.reads["/proc/43/timens_offsets"], 2)
            self.assertNotIn("/proc/thread-self/timens_offsets", fixture.paths)
            self.assertEqual(h.packet(json.dumps(packet).encode() + b"\n", "observer", "observer", NONCE), packet)
            self.assertEqual(fixture.alive, set())
        # The proc mount can show a different ID domain from native IDs.
        with ThreadOffsetFixture(p, mismatch="pid-view") as fixture:
            packet = p.capture("observer", "observer", NONCE)
            self.assertTrue(packet["complete"], packet)
            self.assertEqual(packet["native_tid"], 43)
            self.assertEqual(packet["contexts"]["thread_self"]["offsets_reader"]["path"], "/proc/100")
            self.assertEqual(fixture.reads["/proc/100/timens_offsets"], 2)
            self.assertNotIn("/proc/43/timens_offsets", fixture.paths)
            self.assertFalse(packet["actor_pid_view_matches"])
            self.assertFalse(packet["supported_source_guard"])
            self.assertFalse(packet["shared_time_domain"])
            self.assertEqual(h.packet(json.dumps(packet).encode() + b"\n", "observer", "observer", NONCE), packet)
            self.assertEqual(fixture.alive, set())
        # Independent byte sentinels show that the TID read is neither copied
        # from self nor translated. This controlled matrix is unsupported.
        with ThreadOffsetFixture(p, issue="distinct-offsets", mismatch="children") as fixture:
            packet = p.capture("observer", "observer", NONCE)
            self.assertTrue(packet["complete"], packet)
            thread = packet["contexts"]["thread_self"]["offsets_for_children"]
            own = packet["contexts"]["self"]["offsets_for_children"]
            self.assertEqual(base64.b64decode(thread["raw_base64"]), fixture.distinct_offsets)
            self.assertNotEqual(thread, own)
            self.assertEqual(thread["values"]["boottime"]["seconds"], 23)
            self.assertFalse(packet["supported_source_guard"])
            self.assertFalse(packet["shared_time_domain"])
            self.assertEqual(packet["own_boottime_bracket_ns"], [100, 200])
            self.assertEqual(fixture.alive, set())

    def test_12_unsafe_thread_links_are_unknown_without_offsets_fallback(self):
        p = self.probe
        for link in ("42/task/0", "/42/task/43", "42/task/43/../../self", "42/task/43\n", "2147483648/task/43", True):
            with self.subTest(link=link), ThreadOffsetFixture(p, link_value=link) as fixture:
                packet = p.capture("observer", "observer", NONCE)
                self.assertFalse(packet["complete"], packet)
                self.assertIsNone(packet["shared_time_domain"])
                self.assertIsNone(packet["supported_source_guard"])
                self.assertTrue(packet["errors"])
                self.assertFalse(any(path.endswith("/timens_offsets") for path in fixture.paths))
                self.assertEqual(fixture.alive, set())

    def test_13_thread_reader_transitions_and_missing_facts_are_unknown(self):
        p = self.probe
        for issue in ("link-read", "link-transition", "reader-absent", "reader-pid", "reader-tgid", "birth-transition", "status-transition", "namespace-transition", "children-namespace-transition", "offset-transition", "reader-close"):
            with self.subTest(issue=issue), ThreadOffsetFixture(p, issue=issue) as fixture:
                packet = p.capture("observer", "observer", NONCE)
                self.assertFalse(packet["complete"], packet)
                self.assertIsNone(packet["shared_time_domain"])
                self.assertIsNone(packet["supported_source_guard"])
                self.assertEqual(packet["source_refusal"], "UNKNOWN")
                self.assertTrue(packet["errors"])
                self.assertNotIn("/proc/thread-self/timens_offsets", fixture.paths)
                self.assertEqual(fixture.alive, set())

    def test_14_host_rejects_unbound_or_untyped_offsets_reader_provenance(self):
        p, h = self.probe, self.host
        with ThreadOffsetFixture(p) as fixture:
            good = p.capture("observer", "observer", NONCE)
        self.assertTrue(good["complete"])
        for kind in ("missing", "path", "link", "changed-link", "birth", "status", "namespace", "bool"):
            bad = copy.deepcopy(good)
            row = bad["contexts"]["thread_self"]
            proof = row["offsets_reader"]
            if kind == "missing":
                del row["offsets_reader"]
            elif kind == "path":
                proof["path"] = "/proc/42"
            elif kind == "link":
                proof["thread_self_link_before"] = proof["thread_self_link_after"] = "42/task/44"
            elif kind == "changed-link":
                proof["thread_self_link_after"] = "42/task/44"
            elif kind == "birth":
                proof["after"]["process"]["start_ticks"] += 1
            elif kind == "status":
                proof["before"]["status"]["Tgid"] = [44]
            elif kind == "namespace":
                proof["after"]["time_for_children"][1] += 1
            else:
                # True==1 in Python: equality alone cannot qualify provenance.
                proof["before"]["process"]["ppid"] = False
            with self.subTest(provenance=kind), self.assertRaises((h.Fault, ValueError, KeyError)):
                h.packet(json.dumps(bad).encode() + b"\n", "observer", "observer", NONCE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
