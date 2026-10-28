"""One separately admitted preparer diagnostic; no work occurs on import."""
import json
import math
import os
from pathlib import Path
import secrets
import stat
import subprocess
import sys
import time

import sg32_host as host
from sg32_common import Fault, need, strict_json, encode, read_owned, digest, hex_value, IMAGE

PROFILE = "SG32-PREPARER-ONLY-v1"
PURPOSE = "PREPARER_PERMISSION_DIAGNOSTIC_ONLY"
DIAGNOSTIC_SHA = "4575c4a8d4e18c3d14124beb8a62559fb4f4a4833aac449a88644628b19d26aa"


def admission(path):
    raw, cfg = host.read_admission(path, expected_profile=PROFILE)
    need(cfg.get("purpose") == PURPOSE and cfg.get("runtime_service") is False and
         cfg.get("runtime_driver") is False, "preparer-only-purpose")
    decision_raw = read_owned(cfg["owner_decision_path"], 8192)
    decision = strict_json(decision_raw)
    need(type(decision) is dict and all(type(decision.get(k)) is bool for k in
         ("owner_approved", "one_attempt", "runtime_service", "runtime_driver")) and digest(decision_raw) == cfg["owner_decision_sha256"] and
         decision == {"profile": PROFILE, "purpose": PURPOSE, "owner_approved": True,
                      "one_attempt": True, "runtime_service": False, "runtime_driver": False},
         "new-preparer-owner-decision")
    need(digest(read_owned(__file__, 65536)) == cfg["preparer_only_sha256"] and
         cfg["files"]["sg32_service.py"]["sha256"] == DIAGNOSTIC_SHA, "preparer-source-binding")
    return raw, cfg


class RestrictedEngine(host.Engine):
    """Every Docker argv passes this boundary, including inherited cleanup calls."""
    def __init__(self, nonce, cfg):
        super().__init__()
        self.nonce, self.cfg = nonce, cfg
        self.names = host.volume_names(nonce)
        self.created, self.started, self.prep_id, self.prep_child = False, False, None, None
        self.attempted_copies, self.copied = set(), set()
        self.volumes, self.discovered, self.verified = set(), set(), set()

    def command(self, args, deadline, mutation=False):
        need(type(args) is list and all(type(x) is str for x in args), "closed-command-type")
        n, names = self.nonce, self.names
        scope = ["ps", "-a", "--no-trunc", "--filter", "label=corsa.sg32.owner=" + n, "--format", "{{.ID}}"]
        allowed = args in (["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}", "desktop-linux"],
                          ["info", "--format", "{{json .ServerVersion}}"],
                          ["image", "inspect", "--format", "{{json .}}", IMAGE], scope,
                          ["volume", "ls", "--filter", "label=corsa.sg32.owner=" + n, "--format", "{{.Name}}"])
        expected_mutation = False
        if args == host.create_argv("preparer", n, names):
            need(not self.created and self.volumes == set(names.values()), "one-preparer-create")
            self.created = True
            allowed, expected_mutation = True, True
        for role, name in names.items():
            create = ["volume", "create", "--driver=local", "--label", "corsa.sg32.owner=" + n,
                      "--label", "corsa.sg32.role=" + role]
            if role == "secret":
                create += ["--opt", "type=tmpfs", "--opt", "device=tmpfs", "--opt", "o=size=1048576,mode=0700"]
            if args == create + [name]:
                need(not self.created and name not in self.volumes, "fresh-volume-create-once")
                self.volumes.add(name)
                allowed, expected_mutation = True, True
            if args in (["volume", "ls", "--filter", "name=" + name, "--format", "{{.Name}}"],
                        ["volume", "inspect", "--format", "{{json .}}", name]):
                allowed = True
            if args == ["volume", "rm", name] and name in self.volumes:
                allowed, expected_mutation = True, True
        known = self.discovered | ({self.prep_id} if self.prep_id else set())
        for ident in known:
            if args in (["inspect", "--format", "{{json .}}", ident],
                        ["ps", "-a", "--no-trunc", "--filter", "id=" + ident, "--format", "{{.ID}}"]):
                allowed = True
            if args == ["rm", "-f", ident] and ident in self.verified:
                allowed, expected_mutation = True, True
        uploads = {name: entry["path"] for name, entry in self.cfg["files"].items()}
        if self.root is not None:
            uploads["manifest.json"] = str(self.root / "inputs-manifest.json")
        pending_copy = None
        for name, source in uploads.items():
            if self.prep_id and args == ["cp", source, self.prep_id + ":/inputs/" + name]:
                need(not self.started and name not in self.attempted_copies, "upload-once-before-start")
                self.attempted_copies.add(name)
                pending_copy = name
                allowed, expected_mutation = True, True
        need(allowed and mutation is expected_mutation, "preparer-only-command-denied")
        raw = super().command(args, deadline, mutation=mutation)
        if pending_copy is not None:
            self.copied.add(pending_copy)
        if args == scope:
            found = raw.decode().splitlines()
            need(len(found) <= 1 and all(hex_value(x, 64) for x in found), "unexpected-scoped-container")
            self.discovered.update(found)
        return raw

    def inspect(self, ident, deadline):
        meta = super().inspect(ident, deadline)
        labels = meta["Config"]["Labels"]
        need(labels.get("corsa.sg32.owner") == self.nonce and labels.get("corsa.sg32.role") == "preparer" and
             meta["Name"] == "/corsa-d1-sg32-" + self.nonce + "-preparer", "preparer-owner-only")
        self.verified.add(ident)
        return meta

    def start_preparer(self, deadline):
        need(not self.started and self.prep_id in self.verified and
             self.copied == set(self.cfg["files"]) | {"manifest.json"}, "closed-preparer-start")
        meta = self.inspect(self.prep_id, deadline)
        host.verify_created(meta, "preparer", self.nonce, self.names)
        need(meta["Config"]["Entrypoint"] == ["/usr/bin/python3"] and
             meta["Config"]["Cmd"] == ["-I", "-S", "-B", "-c", host.BOOTSTRAP, "preparer", self.nonce],
             "preparer-entry-command")
        need(time.monotonic() < deadline, "preparer-start-deadline")
        self.started = True
        before = len(self.live)
        try:
            return host.Child(self, ["start", "-a", "-i", self.prep_id], "control", True)
        finally:
            if len(self.live) > before:
                self.prep_child = self.live[before]


def run(path, started_at=None):
    now = time.monotonic()
    began = now if started_at is None else started_at
    need(type(began) in (int, float) and math.isfinite(began) and 0 <= began <= now, "invalid-entry-origin")
    work, end = began + 180, began + 210
    path = Path(path)
    raw, cfg = admission(path)
    need(time.monotonic() < work and (os.getuid(), os.getgid()) == (501, 20), "prefix-or-owner")
    parent = Path(cfg["run_parent"])
    need(parent.is_absolute() and parent.resolve(strict=True) == parent and len(str(parent)) <= 512, "run-parent-path")
    st = parent.stat()
    need(st.st_uid == 501 and stat.S_IMODE(st.st_mode) == 0o700, "run-parent-custody")
    space = os.statvfs(parent)
    need(space.f_bavail * space.f_frsize >= 10 * 1024**3 + sum(x["bytes"] for x in cfg["files"].values()), "free-space")
    owner = dict(line.split("=", 1) for line in read_owned(host.LOCK / "owner", 8192).decode().splitlines() if "=" in line)
    need(owner.get("name") == "d1-sg32" and owner.get("pgid") == str(os.getpgrp()), "own-heavy-group")
    nonce = secrets.token_hex(16)
    engine = RestrictedEngine(nonce, cfg)
    ids, volumes, claimed = [], [], False
    result = {"profile": PROFILE, "nonce": nonce, "status": "NOT_STARTED", "measurement_qualified": False,
              "historical_cause": "UNKNOWN", "physical_closure": "UNKNOWN", "preparer": None}
    try:
        endpoint = strict_json(engine.command(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}", "desktop-linux"], work))
        need(endpoint == host.ENDPOINT, "local-endpoint")
        engine.command(["info", "--format", "{{json .ServerVersion}}"], work)
        image = strict_json(engine.command(["image", "inspect", "--format", "{{json .}}", IMAGE], work))
        need(image["Id"] == IMAGE and image["Os"] == "linux" and image["Architecture"] == "arm64" and
             image["Config"].get("Volumes") in (None, {}), "image-platform-or-volumes")
        engine.root = parent / ("preparer-" + nonce)
        engine.root.mkdir(mode=0o700)
        need(read_owned(path, 65536) == raw, "admission-changed")
        claimed = True
        result["status"] = "INCOMPLETE"
        fd = os.open(path.with_name("PREPARER-ONE-ATTEMPT-CLAIM.json"), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            claim = encode({"nonce": nonce, "profile": PROFILE, "admission_sha256": digest(raw), "retry": False}, 4096)
            need(os.write(fd, claim) == len(claim), "claim-short-write")
            os.fsync(fd)
        finally:
            os.close(fd)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        for role, name in engine.names.items():
            need(not engine.command(["volume", "ls", "--filter", "name=" + name, "--format", "{{.Name}}"], work).strip(), "volume-not-fresh")
            volumes.append(name)
            args = ["volume", "create", "--driver=local", "--label", "corsa.sg32.owner=" + nonce, "--label", "corsa.sg32.role=" + role]
            if role == "secret":
                args += ["--opt", "type=tmpfs", "--opt", "device=tmpfs", "--opt", "o=size=1048576,mode=0700"]
            engine.command(args + [name], work, mutation=True)
            meta = strict_json(engine.command(["volume", "inspect", "--format", "{{json .}}", name], work))
            expected = {"type": "tmpfs", "device": "tmpfs", "o": "size=1048576,mode=0700"} if role == "secret" else None
            need(meta["Name"] == name and meta["Driver"] == "local" and
                 (meta.get("Options") == expected or (expected is None and meta.get("Options") == {})) and
                 meta["Labels"].get("corsa.sg32.owner") == nonce and meta["Labels"].get("corsa.sg32.role") == role, "actual-volume")
        ident = engine.command(host.create_argv("preparer", nonce, engine.names), work, mutation=True).decode().strip()
        need(hex_value(ident, 64), "preparer-id")
        engine.prep_id = ident
        ids.append(ident)
        host.verify_created(engine.inspect(ident, work), "preparer", nonce, engine.names)
        manifest = {"nonce": nonce, "files": {k: {"sha256": x["sha256"], "bytes": x["bytes"]} for k, x in cfg["files"].items()}}
        engine.put("inputs-manifest.json", manifest)
        for name, entry in cfg["files"].items():
            before = read_owned(entry["path"], 128 * 1024**2)
            need(digest(before) == entry["sha256"], "upload-changed")
            engine.command(["cp", entry["path"], ident + ":/inputs/" + name], work, mutation=True)
            need(read_owned(entry["path"], 128 * 1024**2) == before, "upload-changed-after")
        engine.command(["cp", str(engine.root / "inputs-manifest.json"), ident + ":/inputs/manifest.json"], work, mutation=True)
        deadline = min(work, time.monotonic() + 20)
        prep = engine.start_preparer(deadline)
        prep.send(encode(manifest, 4096)); prep.finish_input()
        engine.pump_until(prep.done, deadline)
        result["preparer"] = {"records": None, "cli_exit": prep.p.poll(), "state": None,
                              "child": prep.number, "scope": "before-cleanup"}
        rows = prep.records(True)
        state = engine.inspect(ident, work)["State"]
        result["preparer"].update(records=rows, cli_exit=prep.p.returncode, state=state)
        need(not prep.err and len(rows) == 1 and type(rows[0]) is dict and rows[0].get("nonce") == nonce and state["Running"] is False and
             type(state["ExitCode"]) is int and state["OOMKilled"] is False, "preparer-data-incomplete")
        if rows[0].get("kind") == "PREPARED":
            need(prep.p.returncode == state["ExitCode"] == 0 and rows[0].get("files") == manifest["files"], "prepared-receipt")
            result["status"] = "PREPARATION_COMPLETE"
        else:
            need(rows[0].get("kind") == "ERROR" and prep.p.returncode == state["ExitCode"] == 2, "preparer-error-receipt")
            result["status"] = "PREPARER_ERROR_OBSERVED"
    except (Fault, OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        result["status"] = "INCOMPLETE" if claimed else "NOT_STARTED"
        result["error"] = {"type": type(error).__name__, "reason": str(error)[:128] if isinstance(error, Fault) else "operation-failed"}
        if engine.prep_child is not None:
            child = engine.prep_child
            if result["preparer"] is None:
                result["preparer"] = {"records": None, "state": None, "scope": "before-failure-cleanup"}
            result["preparer"].update(cli_exit=child.p.poll(), child=child.number)
            try:
                result["preparer"]["state"] = engine.inspect(engine.prep_id, min(end, time.monotonic() + 2))["State"]
            except BaseException:
                pass  # Retain explicit unknown state and the original error.
    finally:
        closed = host.cleanup(engine, nonce, ids, volumes, end) if claimed else engine.close_children(end)
        result["physical_closure"] = "KNOWN" if closed else "UNKNOWN"
        if not closed or engine.output_failed:
            result["status"] = "INCOMPLETE" if closed else "RECOVERY_REQUIRED"
        if engine.root is not None:
            try:
                engine.put("commands.json", engine.commands)
                engine.put("result.json", result)
            except BaseException:
                if closed:
                    result["status"] = "INCOMPLETE"
        if not closed:
            try:
                print(json.dumps({"status": "RECOVERY_REQUIRED", "run": str(engine.root)}), flush=True)
            except BaseException:
                pass
            while True:
                time.sleep(3600)
        engine.selector.close()
    print(json.dumps({"status": result["status"], "run": str(engine.root)}), flush=True)
    return 0 if result["status"] == "PREPARATION_COMPLETE" else 2


def main(argv=None, started_at=None):
    args = sys.argv[1:] if argv is None else argv
    need(len(args) == 1, "INERT-admission-path-required")
    return run(Path(args[0]), started_at=started_at)
