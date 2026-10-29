#!/usr/bin/env python3
"""Finite refusal/coverage controls; uses a stub GO, never the real toolchain."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().with_name("test-release.sh")
MODULE = "github.com/piratecash/corsa"
DEFERRED = MODULE + "/internal/overlaysim"
NODE = MODULE + "/internal/core/node"
TIMING_TESTS = ("TestOwnedReadyTargetTimingFourGenuineSources",
                "TestOwnedReadyNormalTimedFactoryLoadsOriginalAndSignsWithoutGate")
NODE_SKIP = "^(" + "|".join(TIMING_TESTS) + ")$"


class ReleaseTestScope(unittest.TestCase):
    def run_scope(self, inventory, list_exit=0, test_exit=0, *, version="2.5.72",
                  node_exit=0, config_bytes=None, extra_env=None):
        with tempfile.TemporaryDirectory(prefix="corsa-release-scope-") as directory:
            root = Path(directory)
            stub = root / "go"
            stub.write_text(
                "#!/bin/sh\n"
                'if [ "$1" = list ]; then cat "$INVENTORY"; exit "$LIST_EXIT"; fi\n'
                'printf "__CALL__\\n" >> "$ARGUMENTS"\n'
                'printf "%s\\n" "$@" >> "$ARGUMENTS"\n'
                'for argument do if [ "$argument" = -skip ]; then exit "$NODE_EXIT"; fi; done\n'
                'exit "$TEST_EXIT"\n'
            )
            stub.chmod(0o700)
            data = root / "inventory"
            data.write_text(inventory)
            arguments = root / "arguments"
            if version is not None:
                config = root / "internal/core/config/config.go"
                config.parent.mkdir(parents=True)
                if config_bytes is None:
                    major, minor, build = version.split(".")
                    config_bytes = (f"ClientVersionMajor = {major}\nClientVersionMinor = {minor}\n"
                                    f"ClientVersionBuild = {build}\n").encode("ascii")
                config.write_bytes(config_bytes)
            env = dict(os.environ, GO=str(stub), INVENTORY=str(data),
                       ARGUMENTS=str(arguments), LIST_EXIT=str(list_exit), TEST_EXIT=str(test_exit),
                       NODE_EXIT=str(node_exit))
            env.update(extra_env or {})
            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True,
                                    text=True, timeout=5, check=False, cwd=root)
            calls = None
            if arguments.exists():
                calls = []
                for line in arguments.read_text().splitlines():
                    if line == "__CALL__":
                        calls.append([])
                    else:
                        self.assertTrue(calls, "stub argument without an invocation boundary")
                        calls[-1].append(line)
            return result, calls

    def test_only_exact_package_is_deferred(self):
        kept = [MODULE, MODULE + "/cmd/corsa-node", MODULE + "/cmd/corsa-desktop",
                MODULE + "/internal/overlaysimulator", DEFERRED + "_extra", DEFERRED + "/subpkg"]
        result, argv = self.run_scope("\n".join(kept[:2] + [DEFERRED] + kept[2:]) + "\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(argv, [["test", "-count=1", "-timeout=15m", *kept]])
        self.assertEqual(result.stderr, f"DEFERRED to D1 (not PASS): {DEFERRED}\n")

    def test_failed_inventory_never_runs_partial_scope(self):
        result, argv = self.run_scope(DEFERRED + "\n" + MODULE + "/cmd/corsa-node\n", list_exit=7)
        self.assertEqual(result.returncode, 7)
        self.assertIsNone(argv)

    def test_test_failure_is_preserved(self):
        result, argv = self.run_scope(DEFERRED + "\n" + MODULE + "/cmd/corsa-node\n", test_exit=9)
        self.assertEqual(result.returncode, 9)
        self.assertIsNotNone(argv)

    def test_ambiguous_or_empty_inventory_refuses(self):
        peer = MODULE + "/cmd/corsa-node"
        cases = ["", DEFERRED + "\n", peer + "\n", DEFERRED + "\n" + DEFERRED + "\n" + peer,
                 DEFERRED + "\n" + peer + "\n" + peer, DEFERRED + "\n\n" + peer,
                 DEFERRED + "\n" + peer + "\r\n", DEFERRED + "\n" + peer + " space",
                 DEFERRED + "\nexample.org/foreign", DEFERRED + "\n" + MODULE + "/a/../b",
                 DEFERRED + "\n" + MODULE + "/a//b", DEFERRED + "\n" + MODULE + "/a/.used;exit"]
        for inventory in cases:
            with self.subTest(inventory=inventory):
                result, argv = self.run_scope(inventory)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(argv)

    def test_v2571_defers_both_exact_tests_only_in_node(self):
        others = [MODULE + "/cmd/corsa-node", NODE + "_extra", NODE + "/subpkg", MODULE + "/sdk"]
        inventory = "\n".join([*others[:2], DEFERRED, NODE, *others[2:]]) + "\n"
        result, calls = self.run_scope(inventory, version="2.5.71")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [["test", "-count=1", "-timeout=15m", *others],
                                 ["test", "-count=1", "-timeout=15m", "-skip", NODE_SKIP, NODE]])
        notices = result.stderr.splitlines()
        self.assertEqual(notices[0], f"DEFERRED to D1 (not PASS): {DEFERRED}")
        self.assertEqual(len(notices), 3)
        for name, notice in zip(TIMING_TESTS, notices[1:]):
            self.assertEqual(notice, f"DEFERRED to D1 for 2.5.71 (not PASS): {name}")

    def test_v2571_node_only_inventory_does_not_run_an_empty_group(self):
        result, calls = self.run_scope(DEFERRED + "\n" + NODE + "\n", version="2.5.71")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, [["test", "-count=1", "-timeout=15m", "-skip", NODE_SKIP, NODE]])

    def test_v2571_each_test_group_failure_propagates(self):
        other = MODULE + "/sdk"
        inventory = "\n".join([DEFERRED, other, NODE]) + "\n"
        for first_exit, node_exit, expected_calls, expected_exit in [(7, 0, 1, 7), (0, 9, 2, 9)]:
            with self.subTest(first_exit=first_exit, node_exit=node_exit):
                result, calls = self.run_scope(inventory, version="2.5.71", test_exit=first_exit,
                                              node_exit=node_exit)
                self.assertEqual(result.returncode, expected_exit)
                self.assertEqual(len(calls), expected_calls)
                self.assertEqual(calls[0], ["test", "-count=1", "-timeout=15m", other])
                if expected_calls == 2:
                    self.assertEqual(calls[1], ["test", "-count=1", "-timeout=15m", "-skip", NODE_SKIP, NODE])

    def test_other_versions_cannot_inherit_the_timing_deferral_from_environment(self):
        kept = [NODE, MODULE + "/sdk"]
        spoof = {"CORSA_RELEASE_VERSION": "2.5.71", "RELEASE_TAG": "v2.5.71",
                 "GITHUB_REF": "refs/tags/v2.5.71", "CORSA_SKIP_D1_TESTS": "1"}
        for version in ("2.5.70", "2.5.72", "3.0.0"):
            with self.subTest(version=version):
                result, calls = self.run_scope("\n".join([DEFERRED, *kept]) + "\n",
                                              version=version, extra_env=spoof)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(calls, [["test", "-count=1", "-timeout=15m", *kept]])
                self.assertEqual(result.stderr, f"DEFERRED to D1 (not PASS): {DEFERRED}\n")
        result, calls = self.run_scope("\n".join([DEFERRED, *kept]) + "\n", version="2.5.71",
                                      extra_env={"CORSA_RELEASE_VERSION": "2.5.72", "RELEASE_TAG": "v2.5.72"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[-1], ["test", "-count=1", "-timeout=15m", "-skip", NODE_SKIP, NODE])

    def test_version_binding_refuses_missing_ambiguous_and_malformed_constants(self):
        valid = b"ClientVersionMajor = 2\nClientVersionMinor = 5\nClientVersionBuild = 71\n"
        cases = [b"", valid + b"ClientVersionMajor = 2\n", valid + b"ClientVersionMinor = 5\n",
                 valid + b"ClientVersionBuild = 71\n", valid.replace(b"Build = 71", b"Build = 71oops"),
                 valid.replace(b"Build = 71", b"Build = 71+0"), valid.replace(b"ClientVersionMinor = 5\n", b"")]
        for config in cases:
            with self.subTest(config=config):
                result, calls = self.run_scope(DEFERRED + "\n" + NODE + "\n", config_bytes=config)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(calls)
        result, calls = self.run_scope(DEFERRED + "\n" + NODE + "\n", version=None)
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(calls)

    def test_v2571_requires_one_exact_node_package(self):
        for inventory in [DEFERRED + "\n" + NODE + "_extra\n",
                          DEFERRED + "\n" + NODE + "/subpkg\n",
                          DEFERRED + "\n" + NODE + "\n" + NODE + "\n"]:
            with self.subTest(inventory=inventory):
                result, calls = self.run_scope(inventory, version="2.5.71")
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(calls)


if __name__ == "__main__":
    unittest.main()
