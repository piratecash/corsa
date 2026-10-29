#!/usr/bin/env python3
"""Controller tests with synthetic headers and stub metadata, not real build evidence."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import plistlib
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
    return (f"binary: go1.27.3\n\tpath\tgithub.com/piratecash/corsa/cmd/corsa-{product}\n"
            f"\tbuild\tGOOS={system}\n\tbuild\tGOARCH={arch}\n"
            f"\tbuild\tvcs.revision={revision}\n\tbuild\tvcs.modified={modified}\n")


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

    def test_bundles_archives_and_no_binary_execution(self):
        for platform, (system, arch) in REPORT.PLATFORMS.items():
            with self.subTest(platform=platform), directory():
                candidate_fixture(platform)
                output, run = subprocess_stubs(system, arch)
                with patch.object(REPORT.subprocess, "check_output", side_effect=output), patch.object(REPORT.subprocess, "run", side_effect=run):
                    data = REPORT.report(platform, SHA)
                self.assertEqual(data["source_version"], "2.5.71")
                self.assertEqual(data["commit"], SHA)
                archive = Path(data["archive"]["path"])
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
