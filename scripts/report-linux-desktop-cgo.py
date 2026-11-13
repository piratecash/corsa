#!/usr/bin/env python3
"""Record the Linux amd64 Desktop candidate's CGO environment in CI logs."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import shlex
import subprocess

TARGET = "./cmd/corsa-desktop"
GO_ENV = ("GOOS", "GOARCH", "GOAMD64", "CGO_ENABLED", "GOVERSION", "CC", "CXX")
PKG_CONFIG_FORMAT = "{{range .CgoPkgConfig}}{{println .}}{{end}}"


def command(*argv):
    return subprocess.check_output(argv, text=True, encoding="utf-8", timeout=60).strip()


def collect(source_sha, *, root=Path("."), run=command, host=None):
    if host is None:
        host = (platform.system(), platform.machine())
    if host != ("Linux", "x86_64"):
        raise ValueError("Desktop CGO receipt requires a Linux x86_64 runner")
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha) or run("git", "rev-parse", "HEAD") != source_sha:
        raise ValueError("Desktop CGO source SHA differs from the checkout")
    dotenv = root / ".env"
    if run("git", "status", "--porcelain") or dotenv.exists() or dotenv.is_symlink():
        raise ValueError("Desktop CGO receipt requires a clean checkout without .env overrides")

    go = json.loads(run("go", "env", "-json", *GO_ENV))
    if any(go.get(key) != value for key, value in {
        "GOOS": "linux", "GOARCH": "amd64", "GOAMD64": "v1", "CGO_ENABLED": "1"
    }.items()):
        raise ValueError("unexpected Go platform or disabled CGO")

    compiler = shlex.split(go["CC"])
    if not compiler:
        raise ValueError("missing Go C compiler")
    triple = run(*compiler, "-dumpmachine")
    if not triple.startswith("x86_64-") or "linux" not in triple.split("-"):
        raise ValueError("C compiler does not target Linux x86_64")

    selected = sorted(set(run(
        "go", "list", "-mod=readonly", "-deps", "-f", PKG_CONFIG_FORMAT, TARGET
    ).split()))
    if not selected or not all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+\-]*", name) for name in selected):
        raise ValueError("missing or invalid Desktop pkg-config module names")

    modules = {}
    for name in selected:
        version = run("pkg-config", "--modversion", name)
        directory = run("pkg-config", "--variable=pcfiledir", name)
        if not version or not Path(directory).is_absolute():
            raise ValueError(f"unresolved pkg-config module: {name}")
        pc = Path(directory) / f"{name}.pc"
        if not pc.is_file():
            raise ValueError(f"pkg-config source file missing: {name}")
        modules[name] = {"version": version, "pc_file": str(pc.resolve()),
                         "pc_sha256": hashlib.sha256(pc.read_bytes()).hexdigest()}

    compiler_version = run(*compiler, "--version").splitlines()
    pkg_config_version = run("pkg-config", "--version")
    if not compiler_version or not pkg_config_version:
        raise ValueError("missing C compiler or pkg-config version")

    return {
        "schema": 1,
        "scope": "candidate-linux-amd64-desktop-cgo-environment",
        "source_sha": source_sha,
        "host": {"system": host[0], "machine": host[1]},
        "go": go,
        "compiler_target": triple,
        "compiler_version": compiler_version[0],
        "compiler_sysroot": run(*compiler, "-print-sysroot"),
        "pkg_config_version": pkg_config_version,
        "pkg_config_modules": modules,
        "makefile_sha256": hashlib.sha256((root / "Makefile").read_bytes()).hexdigest(),
        "go_mod_sha256": hashlib.sha256((root / "go.mod").read_bytes()).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    args = parser.parse_args()
    try:
        result = collect(args.source_sha)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        raise SystemExit(f"Linux Desktop CGO preflight failed: {error}")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
