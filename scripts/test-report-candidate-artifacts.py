#!/usr/bin/env python3
"""Controller tests with synthetic headers and stub metadata, not real build evidence."""
import contextlib
import importlib.util
import io
import itertools
import json
import os
from pathlib import Path
import plistlib
import shutil
import struct
import subprocess
import tempfile
import tarfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location("candidate_report", Path(__file__).with_name("report-candidate-artifacts.py"))
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
SHA = "a" * 40
RUN_ID = "123"


def header(system, arch):
    raw = bytearray(128)
    if system == "linux":
        raw[:6] = b"\x7fELF\x02\x01"
        struct.pack_into("<H", raw, 18, 62)
    elif system == "darwin":
        raw[:4] = b"\xcf\xfa\xed\xfe"
        struct.pack_into("<I", raw, 4, {"amd64": 0x1000007, "arm64": 0x100000C}[arch])
    else:
        raw[:2] = b"MZ"
        struct.pack_into("<I", raw, 60, 64)
        raw[64:68] = b"PE\0\0"
        struct.pack_into("<H", raw, 68, {"amd64": 0x8664, "arm64": 0xAA64}[arch])
        struct.pack_into("<H", raw, 88, 0x20B)
    return bytes(raw)


def metadata(product, system, arch, revision=SHA, modified="false"):
    desktop_settings = ""
    if product == "desktop" and system in ("linux", "darwin"):
        desktop_settings += "\tbuild\tCGO_ENABLED=1\n"
    if product == "desktop" and system == "linux":
        desktop_settings += "\tbuild\tGOAMD64=v1\n"
    return (f"binary: go1.27.3\n\tpath\tgithub.com/piratecash/corsa/cmd/corsa-{product}\n"
            f"\tbuild\tGOOS={system}\n\tbuild\tGOARCH={arch}\n"
            f"\tbuild\tvcs.revision={revision}\n\tbuild\tvcs.modified={modified}\n{desktop_settings}")


@contextlib.contextmanager
def directory():
    previous = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="corsa-candidate-report-") as value:
        os.chdir(value)
        try:
            yield Path(value)
        finally:
            os.chdir(previous)


def candidate_fixture(platform):
    system, arch = REPORT.PLATFORMS[platform]
    config = Path("internal/core/config/config.go")
    config.parent.mkdir(parents=True)
    config.write_text("ClientVersionMajor = 2\nClientVersionMinor = 5\nClientVersionBuild = 71\n")
    Path("dist").mkdir()
    suffix = ".exe" if system == "windows" else ""
    for product in ("node", "desktop"):
        path = Path("dist") / f"corsa-{product}-{platform}{suffix}"
        path.write_bytes(header(system, arch))
        path.chmod(0o755)
    if system == "darwin":
        bundle = Path("dist") / f"Corsa-{platform}.app/Contents"
        (bundle / "MacOS").mkdir(parents=True)
        (bundle / "Resources").mkdir()
        executable = bundle / "MacOS/Corsa"
        executable.write_bytes(header(system, arch))
        executable.chmod(0o755)
        info = plistlib.dumps({"CFBundleIdentifier": "chat.corsa", "CFBundleExecutable": "Corsa",
                              "CFBundlePackageType": "APPL", "CFBundleIconFile": "app-icon.icns",
                              "CFBundleURLTypes": [{"CFBundleURLSchemes": ["corsa"]}]})
        (bundle / "Info.plist").write_bytes(info)
        Path("packaging/macos").mkdir(parents=True)
        Path("packaging/macos/Info.plist").write_bytes(info)
        (bundle / "Resources/app-icon.icns").write_bytes(b"icon")
        Path("assets/icons").mkdir(parents=True)
        Path("assets/icons/app-icon.icns").write_bytes(b"icon")
    elif system == "linux":
        Path("packaging/linux").mkdir(parents=True)
        Path("packaging/linux/corsa.desktop").write_text("desktop entry")
        Path("assets/icons/png").mkdir(parents=True)
        Path("assets/icons/png/app-icon-256.png").write_bytes(b"icon")
    return system, arch


def subprocess_stubs(system, arch, *, actual_sha=SHA, dirty=False):
    def output(command, **kwargs):
        if command == ["git", "rev-parse", "HEAD"]:
            return actual_sha + "\n"
        if command[:3] != ["go", "version", "-m"] or len(command) != 4:
            raise AssertionError(f"unexpected command: {command}")
        product = "desktop" if "desktop" in command[3] else "node"
        return metadata(product, system, arch)

    def run(command, **kwargs):
        if command != ["git", "diff", "--quiet", "HEAD", "--"]:
            raise AssertionError(f"unexpected command: {command}")
        if dirty:
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0)

    return output, run


class CandidateReports(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, GITHUB_RUN_ID=RUN_ID)
        environment.start()
        self.addCleanup(environment.stop)

    def test_all_platform_headers_and_wrong_targets(self):
        with directory():
            path = Path("binary")
            for system, arch in REPORT.PLATFORMS.values():
                with self.subTest(system=system, arch=arch):
                    path.write_bytes(header(system, arch))
                    REPORT.check_header(path, system, arch)
                    path.write_bytes(b"truncated")
                    with self.assertRaises(ValueError):
                        REPORT.check_header(path, system, arch)
            path.write_bytes(header("windows", "arm64"))
            with self.assertRaises(ValueError):
                REPORT.check_header(path, "windows", "amd64")
            path.write_bytes(header("darwin", "arm64"))
            with self.assertRaises(ValueError):
                REPORT.check_header(path, "darwin", "amd64")

    def test_wrong_build_provenance_is_refused(self):
        with directory():
            path = Path("binary")
            path.write_bytes(header("darwin", "arm64"))
            cases = [metadata("desktop", "darwin", "arm64"), metadata("node", "darwin", "amd64"),
                     metadata("node", "linux", "arm64"), metadata("node", "darwin", "arm64", "b" * 40),
                     metadata("node", "darwin", "arm64", modified="true"), "not a Go executable"]
            for output in cases:
                with self.subTest(output=output), patch.object(REPORT.subprocess, "check_output", return_value=output):
                    with self.assertRaises(ValueError):
                        REPORT.inspect_binary(path, "node", "darwin", "arm64", SHA)

    def test_linux_desktop_requires_cgo_and_release_amd64_target(self):
        with directory():
            path = Path("desktop")
            path.write_bytes(header("linux", "amd64"))
            path.chmod(0o700)
            good = metadata("desktop", "linux", "amd64")
            for raw, error in (
                (good.replace("CGO_ENABLED=1", "CGO_ENABLED=0"), "CGO_ENABLED"),
                (good.replace("\tbuild\tCGO_ENABLED=1\n", ""), "CGO_ENABLED"),
                (good.replace("GOAMD64=v1", "GOAMD64=v3"), "GOAMD64"),
                (good + "\tbuild\tCGO_ENABLED=1\n", "duplicate"),
            ):
                with self.subTest(error=error), patch.object(REPORT.subprocess, "check_output", return_value=raw):
                    with self.assertRaisesRegex(ValueError, error):
                        REPORT.inspect_binary(path, "desktop", "linux", "amd64", SHA)

    def test_macos_desktop_requires_cgo_before_packaging(self):
        for arch, release_archive, setting in itertools.product(
            ("arm64", "amd64"), (False, True), ("", "\tbuild\tCGO_ENABLED=0\n")
        ):
            platform = f"darwin-{arch}"
            with self.subTest(platform=platform, release_archive=release_archive, setting=setting), directory():
                candidate_fixture(platform)
                output, run = subprocess_stubs("darwin", arch)

                def no_desktop_cgo(command, **kwargs):
                    raw = output(command, **kwargs)
                    return raw.replace("\tbuild\tCGO_ENABLED=1\n", setting)

                with (patch.object(REPORT.subprocess, "check_output", side_effect=no_desktop_cgo),
                      patch.object(REPORT.subprocess, "run", side_effect=run)):
                    with self.assertRaisesRegex(ValueError, "CGO_ENABLED"):
                        REPORT.report(platform, SHA, release_archive=release_archive)
                self.assertFalse(Path("release" if release_archive else "dist/candidate").exists())

    def test_bundles_archives_and_no_binary_execution(self):
        for (platform, (system, arch)), release_archive in itertools.product(REPORT.PLATFORMS.items(), (False, True)):
            with self.subTest(platform=platform, release_archive=release_archive), directory():
                candidate_fixture(platform)
                output, run = subprocess_stubs(system, arch)
                with patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                    data = REPORT.report(platform, SHA, release_archive=release_archive)
                self.assertEqual(data["source_version"], "2.5.71")
                self.assertEqual(data["commit"], SHA)
                archive = Path(data["archive"]["path"])
                extension = ".zip" if system == "windows" else ".tar.bz2"
                if release_archive:
                    self.assertEqual(archive, Path("release") / f"corsa-{platform}{extension}")
                    receipt = archive.with_suffix(archive.suffix + ".json")
                    self.assertEqual(json.loads(receipt.read_text(encoding="utf-8")),
                                     {"commit": SHA, "platform": platform, "bytes": archive.stat().st_size,
                                      "sha256": REPORT.digest(archive), "run_id": RUN_ID})
                else:
                    self.assertEqual(archive, Path("dist/candidate") / f"candidate-corsa-{platform}-{SHA}{extension}")
                self.assertEqual(data["archive"]["sha256"], REPORT.digest(archive))
                self.assertEqual(len(data["binaries"]), 2)
                if system == "windows":
                    expected = {path.name: path for path in Path("dist").glob("*.exe")}
                    with zipfile.ZipFile(archive) as stream:
                        self.assertEqual(len(stream.namelist()), len(expected))
                        self.assertEqual(set(stream.namelist()), set(expected))
                        for member in stream.infolist():
                            self.assertEqual(stream.read(member), expected[member.filename].read_bytes())
                            self.assertEqual((member.external_attr >> 16) & 0o777,
                                             expected[member.filename].stat().st_mode & 0o777)
                else:
                    if system == "darwin":
                        bundle = Path("dist") / f"Corsa-{platform}.app"
                        paths = [Path("dist") / f"corsa-node-{platform}", bundle, *bundle.rglob("*")]
                        expected = {str(path.relative_to("dist")): path for path in paths}
                    else:
                        prefix = f"corsa-{platform}"
                        expected = {prefix: None, prefix + "/bin": None,
                                    prefix + "/bin/corsa-node": Path("dist") / f"corsa-node-{platform}",
                                    prefix + "/bin/corsa-desktop": Path("dist") / f"corsa-desktop-{platform}",
                                    prefix + "/share": None, prefix + "/share/applications": None,
                                    prefix + "/share/applications/corsa.desktop": Path("packaging/linux/corsa.desktop"),
                                    prefix + "/share/icons": None, prefix + "/share/icons/hicolor": None,
                                    prefix + "/share/icons/hicolor/256x256": None,
                                    prefix + "/share/icons/hicolor/256x256/apps": None,
                                    prefix + "/share/icons/hicolor/256x256/apps/corsa.png": Path("assets/icons/png/app-icon-256.png")}
                    with tarfile.open(archive) as stream:
                        members = stream.getmembers()
                        self.assertEqual(len(members), len(expected))
                        self.assertEqual({member.name for member in members}, set(expected))
                        for member in members:
                            source = expected[member.name]
                            if source is None or source.is_dir():
                                self.assertTrue(member.isdir())
                            else:
                                self.assertTrue(member.isfile())
                                self.assertEqual(stream.extractfile(member).read(), source.read_bytes())
                                self.assertEqual(member.mode & 0o777, source.stat().st_mode & 0o777)
                json.dumps(data)

    def test_packaging_refuses_changed_executable_before_writing_release_receipt(self):
        for platform in ("linux-amd64", "windows-amd64"):
            with self.subTest(platform=platform), directory():
                candidate_fixture(platform)
                system, arch = REPORT.PLATFORMS[platform]
                output, run = subprocess_stubs(system, arch)
                inspect = REPORT.inspect_binary

                def inspect_then_change(path, product, *args):
                    receipt = inspect(path, product, *args)
                    if product == "desktop":
                        body = path.read_bytes()
                        path.write_bytes(body[:-1] + bytes([body[-1] ^ 1]))
                    return receipt

                with patch.object(REPORT.subprocess, "check_output", side_effect=output), \
                     patch.object(REPORT.subprocess, "run", side_effect=run), \
                     patch.object(REPORT, "inspect_binary", side_effect=inspect_then_change):
                    with self.assertRaisesRegex(ValueError, "packed executable differs"):
                        REPORT.report(platform, SHA, release_archive=True)
                self.assertEqual(list(Path("release").glob("*.json")), [])

    def test_packaging_refuses_changed_resources_or_unexpected_members(self):
        cases = (
            ("linux-amd64", "share/applications/corsa.desktop", "change"),
            ("linux-amd64", "share/icons/hicolor/256x256/apps/corsa.png", "remove"),
            ("darwin-arm64", "Contents/Info.plist", "change"),
            ("darwin-amd64", "Contents/Resources/app-icon.icns", "change"),
            ("darwin-arm64", "Contents/Resources/unexpected", "add"),
        )
        for platform, relative, change in cases:
            with self.subTest(platform=platform, relative=relative, change=change), directory():
                system, arch = candidate_fixture(platform)
                output, run = subprocess_stubs(system, arch)
                root = f"corsa-{platform}" if system == "linux" else f"Corsa-{platform}.app"
                add = tarfile.TarFile.add

                def add_with_change(stream, path, arcname=None, recursive=True, *, filter=None):
                    if arcname == root:
                        resource = Path(path) / relative
                        if change == "change":
                            data = resource.read_bytes()
                            resource.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
                        elif change == "remove":
                            resource.unlink()
                        else:
                            resource.write_bytes(b"extra member")
                    return add(stream, path, arcname, recursive, filter=filter)

                with patch.object(REPORT.subprocess, "check_output", side_effect=output), \
                     patch.object(REPORT.subprocess, "run", side_effect=run), \
                     patch.object(tarfile.TarFile, "add", new=add_with_change):
                    with self.assertRaisesRegex(ValueError, "packed resource differs|missing paths|unexpected path"):
                        REPORT.report(platform, SHA, release_archive=True)
                self.assertEqual(list(Path("release").glob("*.json")), [])

    def test_collect_release_requires_exact_groups_names_commit_and_bytes(self):
        cases = ("valid", "missing-group", "extra-group", "extra-file", "missing-receipt",
                 "wrong-commit", "wrong-platform", "wrong-size", "wrong-run", "changed-bytes",
                 "symlink", "duplicate-field", "oversized-receipt", "non-integer-size")
        for case in cases:
            with self.subTest(case=case), directory():
                expected = {}
                for platform, group in REPORT.RELEASE_ARTIFACTS.items():
                    folder = Path("release-artifacts") / group
                    folder.mkdir(parents=True)
                    extension = ".zip" if platform.startswith("windows-") else ".tar.bz2"
                    archive = folder / f"corsa-{platform}{extension}"
                    archive.write_bytes(platform.encode())
                    receipt = archive.with_suffix(archive.suffix + ".json")
                    fields = {"commit": SHA, "platform": platform, "bytes": archive.stat().st_size,
                              "sha256": REPORT.digest(archive), "run_id": RUN_ID}
                    receipt.write_text(json.dumps(fields), encoding="utf-8")
                    expected[archive.name] = fields["sha256"]
                if case == "missing-group":
                    archive.unlink()
                    receipt.unlink()
                    folder.rmdir()
                elif case == "extra-group":
                    (folder.parent / "extra").mkdir()
                elif case == "extra-file":
                    (folder / "extra.zip").write_bytes(b"unverified")
                elif case == "missing-receipt":
                    receipt.unlink()
                elif case in {"wrong-commit", "wrong-platform", "wrong-size", "wrong-run"}:
                    key, value = {"wrong-commit": ("commit", "b" * 40), "wrong-platform": ("platform", "linux-amd64"),
                                  "wrong-size": ("bytes", 1), "wrong-run": ("run_id", "124")}[case]
                    receipt.write_text(json.dumps({**fields, key: value}), encoding="utf-8")
                elif case == "duplicate-field":
                    receipt.write_text(json.dumps(fields)[:-1] + f', "run_id": "{RUN_ID}"}}', encoding="utf-8")
                elif case == "oversized-receipt":
                    receipt.write_text(json.dumps(fields) + " " * 4096, encoding="utf-8")
                elif case == "non-integer-size":
                    receipt.write_text(json.dumps({**fields, "bytes": float(fields["bytes"])}), encoding="utf-8")
                elif case == "changed-bytes":
                    archive.write_bytes(b"x" * fields["bytes"])
                elif case == "symlink":
                    backup = Path("elsewhere.zip")
                    archive.rename(backup)
                    archive.symlink_to(backup.resolve())

                output, run = subprocess_stubs("linux", "amd64")
                with patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                    if case == "valid":
                        self.assertEqual(REPORT.collect_release(SHA), {"commit": SHA, "run_id": RUN_ID, "archives": expected})
                        self.assertEqual({p.name: REPORT.digest(p) for p in Path("dist").iterdir()}, expected)
                    else:
                        with self.assertRaises(ValueError):
                            REPORT.collect_release(SHA)
                        self.assertFalse(Path("dist").exists())

    def test_collect_release_consumes_all_five_packager_outputs(self):
        with directory() as publish_root:
            inbox = publish_root / "release-artifacts"
            expected = {}
            for platform, group in REPORT.RELEASE_ARTIFACTS.items():
                with directory():
                    system, arch = candidate_fixture(platform)
                    output, run = subprocess_stubs(system, arch)
                    with patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                        package = REPORT.report(platform, SHA, release_archive=True)
                    archive = Path(package["archive"]["path"])
                    receipt = archive.with_suffix(archive.suffix + ".json")
                    downloaded = inbox / group
                    downloaded.mkdir(parents=True)
                    shutil.copyfile(archive, downloaded / archive.name)
                    shutil.copyfile(receipt, downloaded / receipt.name)
                    expected[archive.name] = package["archive"]["sha256"]

            output, run = subprocess_stubs("linux", "amd64")
            with patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                self.assertEqual(REPORT.collect_release(SHA), {"commit": SHA, "run_id": RUN_ID, "archives": expected})
            self.assertEqual({path.name: REPORT.digest(path) for path in Path("dist").iterdir()}, expected)

    def test_release_archive_modes_require_a_run_id(self):
        with directory(), patch.dict(os.environ, GITHUB_RUN_ID=""):
            with self.assertRaisesRegex(ValueError, "workflow run ID"):
                REPORT.report("linux-amd64", SHA, release_archive=True)
            with self.assertRaisesRegex(ValueError, "workflow run ID"):
                REPORT.collect_release(SHA)
            self.assertFalse(Path("release").exists())
            self.assertFalse(Path("dist").exists())

    def test_release_ref_accepts_lightweight_and_annotated_tags(self):
        ref = "refs/tags/v2.5.71"
        for response in (f"{SHA}\t{ref}\n", f"{'b' * 40}\t{ref}\n{SHA}\t{ref}^{{}}\n"):
            with self.subTest(response=response), patch.dict(os.environ, GITHUB_REF=ref):
                with patch.object(REPORT.subprocess, "check_output", return_value=response) as query:
                    self.assertEqual(REPORT.verify_release_ref(SHA), ref)
                self.assertEqual(query.call_args.args[0],
                                 ["git", "ls-remote", "--exit-code", "origin", ref, ref + "^{}"])
                self.assertEqual(query.call_args.kwargs["env"]["GIT_TERMINAL_PROMPT"], "0")

    def test_release_ref_refuses_moved_missing_or_unverifiable_tags(self):
        ref = "refs/tags/v2.5.71"
        cases = (
            ("branch", "refs/heads/dev", "", "requires a version tag"),
            ("nonversion tag", "refs/tags/latest", "", "requires a version tag"),
            ("moved", ref, f"{'b' * 40}\t{ref}\n", "no longer points"),
            ("moved annotated tag", ref, f"{SHA}\t{ref}\n{'b' * 40}\t{ref}^{{}}\n", "no longer points"),
            ("deleted", ref, "", "no longer points"),
            ("peeled without tag", ref, f"{SHA}\t{ref}^{{}}\n", "no longer points"),
            ("duplicate", ref, f"{SHA}\t{ref}\n{SHA}\t{ref}\n", "duplicate"),
            ("other tag", ref, f"{SHA}\trefs/tags/v2.5.70\n", "unexpected"),
            ("invalid SHA", ref, f"not-a-sha\t{ref}\n", "invalid"),
            ("Git query failed", ref, subprocess.CalledProcessError(2, "git"), "could not verify"),
            ("Git query timed out", ref, subprocess.TimeoutExpired("git", 30), "could not verify"),
        )
        for name, event_ref, response, error in cases:
            with self.subTest(case=name), patch.dict(os.environ, GITHUB_REF=event_ref):
                kwargs = {"side_effect": response} if isinstance(response, Exception) else {"return_value": response}
                with patch.object(REPORT.subprocess, "check_output", **kwargs) as query:
                    with self.assertRaisesRegex(ValueError, error):
                        REPORT.verify_release_ref(SHA)
                if event_ref != ref:
                    query.assert_not_called()

    def test_checkout_and_private_environment_refuse(self):
        for case in ("wrong-head", "dirty", "private-env", "short-sha"):
            with self.subTest(case=case), directory():
                system, arch = candidate_fixture("darwin-arm64")
                if case == "private-env":
                    Path(".env").write_text("PRIVATE=fixture\n")
                output, run = subprocess_stubs(system, arch, actual_sha="b" * 40 if case == "wrong-head" else SHA,
                                              dirty=case == "dirty")
                with patch.object(REPORT.subprocess, "check_output", side_effect=output) as calls, patch.object(REPORT.subprocess, "run", side_effect=run):
                    with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                        REPORT.report("darwin-arm64", SHA[:7] if case == "short-sha" else SHA)
                self.assertFalse(any(call.args[0][:3] == ["go", "version", "-m"] for call in calls.call_args_list))
                self.assertFalse(Path("dist/candidate").exists())

    def test_macos_bundle_corruption_refuses(self):
        for case in ("exe-bytes", "plist-id", "plist-executable", "plist-scheme", "icon", "symlink"):
            with self.subTest(case=case), directory():
                system, arch = candidate_fixture("darwin-arm64")
                bundle = Path("dist/Corsa-darwin-arm64.app/Contents")
                if case == "exe-bytes":
                    (bundle / "MacOS/Corsa").write_bytes(b"different")
                elif case.startswith("plist-"):
                    path = bundle / "Info.plist"
                    info = plistlib.loads(path.read_bytes())
                    if case == "plist-id":
                        info["CFBundleIdentifier"] = "example.wrong"
                    elif case == "plist-executable":
                        info["CFBundleExecutable"] = "MissingExecutable"
                    else:
                        info["CFBundleURLTypes"] = []
                    path.write_bytes(plistlib.dumps(info))
                elif case == "icon":
                    (bundle / "Resources/app-icon.icns").write_bytes(b"wrong icon")
                else:
                    (bundle / "Resources/link").symlink_to("app-icon.icns")
                output, run = subprocess_stubs(system, arch)
                with patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                    with self.assertRaises(ValueError):
                        REPORT.report("darwin-arm64", SHA)

    def test_unix_candidates_must_be_executable(self):
        for platform in ("linux-amd64", "darwin-arm64"):
            targets = [f"dist/corsa-node-{platform}", f"dist/corsa-desktop-{platform}"]
            if platform == "darwin-arm64":
                targets.append("dist/Corsa-darwin-arm64.app/Contents/MacOS/Corsa")
            for target in targets:
                with self.subTest(target=target), directory():
                    system, arch = candidate_fixture(platform)
                    Path(target).chmod(0o644)
                    output, run = subprocess_stubs(system, arch)
                    with patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                        with self.assertRaises(ValueError):
                            REPORT.report(platform, SHA)

    def test_windows_non_ascii_source_uses_explicit_utf8(self):
        original_text_encoding = io.text_encoding

        def windows_text_encoding(encoding, stacklevel=2):
            return "cp1252" if encoding is None else original_text_encoding(encoding, stacklevel)

        for platform in ("windows-amd64", "windows-arm64"):
            with self.subTest(platform=platform), directory():
                system, arch = candidate_fixture(platform)
                config = Path("internal/core/config/config.go")
                config_bytes = "// я: UTF-8 source, never the Windows locale\n".encode("utf-8") + config.read_bytes()
                self.assertIn(b"\x8f", config_bytes)
                config.write_bytes(config_bytes)
                base_output, run = subprocess_stubs(system, arch)

                def output(command, **kwargs):
                    value = base_output(command, **kwargs)
                    if command[:3] == ["go", "version", "-m"]:
                        # Real UTF-8 decoding of synthetic Go metadata. No
                        # subprocess is launched; an omitted encoding models
                        # the same CP1252 Windows boundary as Path.read_text.
                        value += "\tbuild\t-ldflags=я\n"
                    return value.encode("utf-8").decode(kwargs.get("encoding") or "cp1252",
                                                       errors=kwargs.get("errors") or "strict")

                with patch.object(io, "text_encoding", side_effect=windows_text_encoding), patch.object(REPORT.subprocess, "check_output", side_effect=output) as calls, patch.object(REPORT.subprocess, "run", side_effect=run):
                    data = REPORT.report(platform, SHA)
                self.assertEqual(data["source_version"], "2.5.71")
                self.assertEqual(len(calls.call_args_list), 3)
                self.assertEqual(calls.call_args_list[0].args[0], ["git", "rev-parse", "HEAD"])
                for call in calls.call_args_list:
                    self.assertIn(call.kwargs.get("encoding"), ("utf-8", "utf8"))
                    self.assertEqual(call.kwargs.get("errors") or "strict", "strict")
                for binary in data["binaries"]:
                    self.assertIn("-ldflags=я", binary["go_build_info"])
                self.assertTrue(Path(data["archive"]["path"]).is_file())

    def test_windows_invalid_utf8_is_refused(self):
        original_text_encoding = io.text_encoding

        def windows_text_encoding(encoding, stacklevel=2):
            return "cp1252" if encoding is None else original_text_encoding(encoding, stacklevel)

        for platform in ("windows-amd64", "windows-arm64"):
            for boundary in ("config", "go-metadata"):
                with self.subTest(platform=platform, boundary=boundary), directory():
                    system, arch = candidate_fixture(platform)
                    config = Path("internal/core/config/config.go")
                    config.write_bytes((b"// \xff invalid UTF-8\n" if boundary == "config" else b"// valid UTF-8\n") + config.read_bytes())
                    base_output, run = subprocess_stubs(system, arch)

                    def output(command, **kwargs):
                        raw = base_output(command, **kwargs).encode("utf-8")
                        if boundary == "go-metadata" and command[:3] == ["go", "version", "-m"]:
                            raw += b"\tbuild\t-ldflags=\xff\n"
                        return raw.decode(kwargs.get("encoding") or "cp1252",
                                          errors=kwargs.get("errors") or "strict")

                    with patch.object(io, "text_encoding", side_effect=windows_text_encoding), patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                        with self.assertRaises(UnicodeDecodeError):
                            REPORT.report(platform, SHA)
                    self.assertFalse(Path("dist/candidate").exists())


if __name__ == "__main__":
    unittest.main()
