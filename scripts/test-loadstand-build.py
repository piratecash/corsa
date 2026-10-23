#!/usr/bin/env python3
"""T5g pure contract fixtures; Linux kernel controls are separately selected.

No Go/Git/compiler/build/runtime proof is produced by these tests. On Darwin
LinuxSupervisorControls are explicitly pending, not credited as passed.
"""
import ast
import base64
import contextlib
import copy
import dataclasses
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import platform
import re
import signal
import stat
import struct
import sys
import tempfile
import time
import types
import unittest
import zipfile
from unittest import mock

sys.dont_write_bytecode = True
SCRIPT = pathlib.Path(__file__).with_name('loadstand-build.py')
SCHEMA = SCRIPT.parent.parent/'docs/design/loadstand-build-provenance.schema.json'
spec = importlib.util.spec_from_file_location('loadstand_build', SCRIPT)
build = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = build
spec.loader.exec_module(build)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def reference_h1(contents):
    # Agreement vector from x/mod v0.41.0 sumdb/dirhash/hash_test.go TestHash1.
    transcript = b''.join((sha(contents[name])+'  '+name+'\n').encode() for name in sorted(contents))
    return 'h1:'+base64.b64encode(hashlib.sha256(transcript).digest()).decode()


def value_for(field):
    kind = field['kind']
    if kind == 'nullable':
        return None
    if kind == 'ref':
        return sample(field['name'])
    if kind == 'enum':
        return field['values'][0]
    if kind == 'bool':
        return False
    if kind == 'int':
        return field['min']
    if kind == 'array':
        return [value_for(field['item']) for _ in range(field['min'])]
    if kind == 'path':
        return 'x'
    if kind == 'absolute':
        return '/x'
    if field.get('pattern') == build.HASH:
        return 'a'*64
    if field.get('pattern') == build.OID:
        return 'b'*40
    if field.get('pattern') == build.MODULE_VERSION:
        return 'v1.0.0'
    return 'x'


def sample(name):
    value = {key: value_for(field) for key, field in build.DEFS[name].items()}
    if name == 'image':
        value.update(state='absent_native_host', digest=None)
    elif name == 'origin':
        value.update(commit='b'*40, tree='c'*40, blob='d'*40,
                     staged_capture_manifest_sha256=None, role=None)
    elif name == 'overlay':
        value['before'] = None
        value['after'] = sample('blob')
        value['origin']['blob'] = value['after']['oid']
    elif name == 'request':
        value.update(flavour='cross-compile-cgo0', native=None)
    elif name == 'evidence':
        value.update(state='present_verified', sha256='a'*64, reason=None)
    elif name == 'lane':
        value = build.not_started_lane(0)
    elif name == 'receipt':
        request = sample('request')
        raw = build.canonical(request)
        value = build.empty_receipt(request, raw)
        value.update(status='verified_equal', failure=None, comparison='equal')
        for key in ('source', 'toolchain', 'offline'):
            value[key] = sample('evidence')
        value['native'] = build.evidence('unavailable', reason='CompileOnly_CGO0')
        value['cleanup'] = {'state': 'complete', 'possible_writers': False}
        value['supervisor'] = {'state': 'present_verified', 'all_joined': True, 'commands': 1, 'reason': None}
        value['lanes'] = []
        value['artifacts'] = []
        for n in (0, 1):
            lane = {'index': n, 'state': 'complete', 'cache_initially_empty': True,
                    'joined': True, 'targets': request['targets']}
            lane.update({key: 'a'*64 for key in ('source_before', 'source_after', 'seed_before',
                'seed_after', 'toolchain_before', 'toolchain_after', 'closure_sha256')})
            value['lanes'].append(lane)
            artifact = sample('artifact')
            artifact.update(lane=n, size=1)
            value['artifacts'].append(artifact)
    elif name == 'native_file':
        value.update(kind='regular', regular=sample('native_regular'), link=None)
    elif name == 'command':
        value.update(exit_code=0, term_signal=None, joined=True, failure=None)
    elif name == 'seed_module':
        value['path'] = 'example.org/x'
    return value


class ParserControls(unittest.TestCase):
    def test_duplicate_keys_not_last_writer_wins(self):
        for raw in (b'{"v":2,"v":2}', b'{"nested":{"v":1,"v":2}}'):
            with self.assertRaises(build.Failure):
                build.decode_json(raw)

    def test_nonfinite_float_boolean_integer(self):
        for raw in (b'NaN', b'Infinity', b'-Infinity', b'2.0', b'1e3'):
            with self.subTest(raw=raw), self.assertRaises(build.Failure):
                build.decode_json(raw)
        for version in (True, False, 2.0, '2', None):
            with self.subTest(version=version), self.assertRaises(build.Failure):
                build.validate_version(version)
        self.assertEqual(2, build.validate_version(2))

    def test_bounded_preparse_depth_values_utf8(self):
        with self.assertRaises(build.Failure):
            build.decode_json(b'{} ', 2)
        with self.assertRaises(build.Failure):
            build.decode_json(b'['*17+b'0'+b']'*17)
        self.assertEqual('[[[', build.decode_json(b'"[[["'))
        for raw in (b'"\xff"', b'"\\ud800"'):
            with self.assertRaises(build.Failure):
                build.decode_json(raw)
        with mock.patch.dict(build.LIMITS, {'values': 5}):
            with self.assertRaises(build.Failure):
                build.decode_json(b'[1,2,3,4,5]')

    def test_exact_keys_types_null_counts_and_zero_hash(self):
        for name in build.DEFS:
            original = sample(name)
            with self.subTest(name=name):
                build.validate(name, original)
                extra = copy.deepcopy(original)
                extra['unknown'] = None
                with self.assertRaises(build.Failure):
                    build.validate(name, extra)
                for key in original:
                    missing = copy.deepcopy(original)
                    missing.pop(key)
                    with self.assertRaises(build.Failure):
                        build.validate(name, missing)
        for digest in ('0'*64, 'A'*64, 'a'*63, 'a'*65):
            value = sample('evidence')
            value['sha256'] = digest
            with self.assertRaises(build.Failure):
                build.validate('evidence', value)
        bad = sample('request')
        bad['targets'] *= 2
        with self.assertRaises(build.Failure):
            build.validate('request', bad)
        bad = sample('file')
        bad['size'] = True
        with self.assertRaises(build.Failure):
            build.validate('file', bad)

    def test_hash_domains_raw_vs_canonical(self):
        a, b = b'{"x":1}', b'{ "x" : 1 }'
        self.assertNotEqual(sha(a), sha(b))
        self.assertEqual(build.domain_hash('request.canonical', build.decode_json(a)),
                         build.domain_hash('request.canonical', build.decode_json(b)))
        self.assertNotEqual(build.domain_hash('seed', {'x': 1}), build.domain_hash('closure', {'x': 1}))
        self.assertEqual(sha(b'corsa.t5g.v2.outputs\0{"x":1}'), build.domain_hash('outputs', {'x': 1}))
        with self.assertRaises(build.Failure):
            build.domain_hash('receipt', {})

    def test_paths_and_native_flavour_are_not_coerced(self):
        for path in ('../x', '/x', 'a//b', 'a/./b', 'x\n', 'x\\y', 'a/'+'b/'*32+'z'):
            with self.subTest(path=path), self.assertRaises(build.Failure):
                build.relative_path(path)
        request = sample('request')
        request['flavour'] = 'native-linux-arm64'
        with self.assertRaises(build.Failure):
            build.validate('request', request)
        request['native'] = sample('native_request')
        build.validate('request', request)


def schema_validate(value, schema, defs, _budget=None, _depth=0):
    """Independent bounded interpreter for ONLY the checked-in schema subset.

    No external refs/plugins; every unsupported keyword is an assertion failure.
    Dynamic ownership/digest equality remains the explicit runtime semantic gate.
    """
    if _budget is None: _budget = [0]
    _budget[0] += 1
    if _depth > 64 or _budget[0] > 100000: raise AssertionError('bounded schema evaluation')
    def bounded(value, schema, defs):
        return schema_validate(value, schema, defs, _budget, _depth+1)
    supported = {'$schema', '$id', '$comment', '$defs', '$ref', 'type', 'enum', 'const',
                 'required', 'properties', 'additionalProperties', 'oneOf', 'allOf',
                 'if', 'then', 'else', 'items', 'minItems', 'maxItems', 'uniqueItems',
                 'minLength', 'maxLength', 'pattern', 'minimum', 'maximum'}
    if set(schema)-supported:
        raise AssertionError('unsupported schema keyword '+repr(set(schema)-supported))
    if '$ref' in schema:
        ref = schema['$ref']
        if not ref.startswith('#/$defs/'):
            raise AssertionError('external ref')
        return bounded(value, defs[ref[8:]], defs)
    equal = lambda a, b: type(a) is type(b) and a == b
    if 'enum' in schema and not any(equal(value, x) for x in schema['enum']):
        return False
    if 'const' in schema and not equal(value, schema['const']):
        return False
    types = {'object': dict, 'array': list, 'string': str, 'integer': int, 'boolean': bool, 'null': type(None)}
    if 'type' in schema and type(value) is not types[schema['type']]:
        return False
    if 'oneOf' in schema and sum(bounded(value, x, defs) for x in schema['oneOf']) != 1:
        return False
    if 'allOf' in schema and not all(bounded(value, x, defs) for x in schema['allOf']):
        return False
    if 'if' in schema:
        branch = 'then' if bounded(value, schema['if'], defs) else 'else'
        if branch in schema and not bounded(value, schema[branch], defs):
            return False
    if type(value) is dict:
        properties = schema.get('properties', {})
        if not set(schema.get('required', [])) <= set(value):
            return False
        if schema.get('additionalProperties') is False and not set(value) <= set(properties):
            return False
        if any(not bounded(v, properties[k], defs) for k, v in value.items() if k in properties):
            return False
    if type(value) is list:
        if not schema.get('minItems', 0) <= len(value) <= schema.get('maxItems', sys.maxsize):
            return False
        if schema.get('uniqueItems') and len({json.dumps(x, sort_keys=True) for x in value}) != len(value):
            return False
        if 'items' in schema and not all(bounded(x, schema['items'], defs) for x in value):
            return False
    if type(value) is str:
        if not schema.get('minLength', 0) <= len(value) <= schema.get('maxLength', sys.maxsize):
            return False
        if 'pattern' in schema and re.search(schema['pattern'], value) is None:
            return False
    if type(value) is int:
        if value < schema.get('minimum', value) or value > schema.get('maximum', value):
            return False
    return True


class SchemaControls(unittest.TestCase):
    def test_checked_in_schema_matches_tables_and_rejects_unknown_keyword(self):
        document = json.loads(SCHEMA.read_text())
        self.assertEqual(build.schema_document(), document)
        self.assertEqual(set(build.DEFS), set(document['$defs']))
        for name, fields in build.DEFS.items():
            declared = document['$defs'][name]
            self.assertEqual(set(fields), set(declared['properties']))
            self.assertEqual(set(fields), set(declared['required']))
            self.assertIs(False, declared['additionalProperties'])
        with self.assertRaises(AssertionError):
            schema_validate({}, {'silently_ignored': True}, {})

    def test_independent_subset_interpreter_same_structural_field_mutants(self):
        document = json.loads(SCHEMA.read_text())
        defs = document['$defs']
        for name, fields in build.DEFS.items():
            original = sample(name)
            with self.subTest(name=name):
                self.assertTrue(schema_validate(original, defs[name], defs))
                build.validate(name, original)
                bad = copy.deepcopy(original)
                bad['unknown'] = 'x'
                self.assertFalse(schema_validate(bad, defs[name], defs))
                with self.assertRaises(build.Failure):
                    build.validate(name, bad)
                for key, field in fields.items():
                    bad = copy.deepcopy(original)
                    bad.pop(key)
                    self.assertFalse(schema_validate(bad, defs[name], defs))
                    with self.assertRaises(build.Failure):
                        build.validate(name, bad)
                    bad = copy.deepcopy(original)
                    # An object cannot be any primitive/enum/ref except ref;
                    # refs get an integer which cannot be a closed object.
                    actual = field['item'] if field['kind'] == 'nullable' else field
                    bad[key] = 42 if actual['kind'] == 'ref' else {'wrong': True}
                    self.assertFalse(schema_validate(bad, defs[name], defs))
                    with self.assertRaises(build.Failure):
                        build.validate(name, bad)


class ReceiptControls(unittest.TestCase):
    def test_same_source_conjunction_not_two_independent_sources(self):
        good = sample('receipt')
        build.validate('receipt', good)
        for field in ('source_before', 'source_after'):
            bad = copy.deepcopy(good)
            bad['lanes'][1][field] = 'd'*64
            with self.assertRaises(build.Failure):
                build.validate('receipt', bad)
        bad = copy.deepcopy(good)
        bad['lanes'][1].update(source_before='d'*64, source_after='d'*64)
        with self.assertRaises(build.Failure):
            build.validate('receipt', bad)

    def test_no_verified_missing_lanes_artifacts_logs_join_or_cleanup(self):
        original = sample('receipt')
        mutants = []
        bad = copy.deepcopy(original); bad['lanes'][1] = build.not_started_lane(1); mutants.append(bad)
        bad = copy.deepcopy(original); bad['artifacts'].pop(); mutants.append(bad)
        bad = copy.deepcopy(original); bad['supervisor']['all_joined'] = False; mutants.append(bad)
        bad = copy.deepcopy(original); bad['cleanup']['possible_writers'] = True; mutants.append(bad)
        bad = copy.deepcopy(original); bad['lanes'][0]['cache_initially_empty'] = False; mutants.append(bad)
        bad = copy.deepcopy(original); bad['environment']['cgo'] = 1; mutants.append(bad)
        bad = copy.deepcopy(original); bad['artifacts'][1]['sha256'] = 'c'*64; mutants.append(bad)
        bad = copy.deepcopy(original); bad['lanes'][0]['seed_after'] = 'c'*64; mutants.append(bad)
        for n, value in enumerate(mutants):
            with self.subTest(n=n), self.assertRaises(build.Failure):
                build.validate('receipt', value)
        with self.assertRaises(build.Failure):
            build.validate_terminal(original, sample('request'), build.canonical(sample('request')),
                                    {'schema_version': 2, 'files': []})

    def test_failed_incomplete_roundtrip_and_no_cycle(self):
        request = sample('request')
        receipt = build.empty_receipt(request, build.canonical(request))
        for status in ('failed', 'incomplete'):
            receipt['status'] = status
            self.assertEqual(receipt, build.validate('receipt', build.decode_json(build.canonical(receipt))))
        output = {'schema_version': 2, 'files': []}
        build.validate('outputs', output)
        for path in ('receipt.json', 'receipt.sha256', 'artifacts-manifest.json', 'runtime-receipt.json'):
            item = sample('output_file'); item['path'] = path
            with self.assertRaises(build.Failure):
                build.validate('outputs', {'schema_version': 2, 'files': [item]})

    def test_immutable_command_result_and_discriminants(self):
        result = build.CommandResult(1, ('/tool', '--version'), 'a.stdout', 'a.stderr', 0,
                                    None, True, None, ())
        value = result.as_json()
        value['argv'].append('mutable')
        self.assertEqual(('/tool', '--version'), result.argv)
        with self.assertRaises(Exception):
            result.joined = False
        for changed in ({'joined': False}, {'exit_code': None}, {'term_signal': 9}):
            bad = result.as_json(); bad.update(changed)
            with self.assertRaises(build.Failure):
                build.validate('command', bad)


class FixtureObjects:
    """Toy objects only: no real Git repository/caller/CLI authority."""
    commit = build.GitObjects.commit
    path = build.GitObjects.path

    def __init__(self):
        self.objects = {}

    def put(self, kind, raw):
        oid = build.git_oid(kind, raw)
        self.objects[oid] = (kind, raw)
        return oid

    def object(self, oid, kind):
        actual_kind, raw = self.objects[oid]
        if actual_kind != kind or build.git_oid(kind, raw) != oid:
            raise build.Failure('GitObjectIdentity')
        return raw

    def tree(self, values):
        raw = b''.join(mode.encode()+b' '+name.encode()+b'\0'+bytes.fromhex(oid)
                       for name, (mode, oid) in sorted(values.items()))
        return self.put('tree', raw)

    def make_commit(self, tree):
        return self.put('commit', b'tree '+tree.encode()+b'\nauthor T <t@e> 1 +0000\ncommitter T <t@e> 1 +0000\n\nfixture\n')


def fixture_source():
    provider = FixtureObjects()
    mod = provider.put('blob', b'module example.org/main\n')
    sums = provider.put('blob', b'')
    tree = provider.tree({'go.mod': ('100644', mod), 'go.sum': ('100644', sums)})
    commit = provider.make_commit(tree)
    request = sample('request')
    request['git'].update(base_commit=commit, base_tree=tree)
    return provider, request


class GitOriginControls(unittest.TestCase):
    def test_independent_object_hash_commit_tree_blob(self):
        raw = b'hello\n'
        self.assertEqual(hashlib.sha1(b'blob 6\0hello\n').hexdigest(), build.git_oid('blob', raw))
        provider, request = fixture_source()
        plan = build.SourcePlan(provider, request, build.Budget())
        self.assertEqual({'go.mod', 'go.sum'}, set(plan.entries))
        bad = copy.deepcopy(request); bad['git']['base_tree'] = 'e'*40
        with self.assertRaises(build.Failure):
            build.SourcePlan(provider, bad, build.Budget())
        bad_oid = provider.put('blob', b'x')
        provider.objects[bad_oid] = ('blob', b'y')
        with self.assertRaises(build.Failure):
            provider.object(bad_oid, 'blob')

    def test_versioned_and_staged_origins_do_not_launder_old_release(self):
        provider, request = fixture_source()
        raw = b'module example.org/new\n'
        blob = provider.put('blob', raw)
        tree = provider.tree({'go.mod': ('100644', blob), 'go.sum': ('100644', provider.put('blob', b''))})
        commit = provider.make_commit(tree)
        before = build.SourcePlan(provider, request, build.Budget())._binding(
            build.SourcePlan(provider, request, build.Budget()).entries['go.mod'])
        after = {'oid': blob, 'sha256': sha(raw), 'size': len(raw), 'mode': '100644'}
        overlay = {'op': 'modify', 'path': 'go.mod', 'before': before, 'after': after,
                   'origin': {'kind': 'versioned', 'commit': commit, 'tree': tree, 'path': 'go.mod',
                              'blob': blob, 'staged_capture_manifest_sha256': None, 'role': 'harness'}}
        request['overlay'] = [overlay]
        plan = build.SourcePlan(provider, request, build.Budget())
        self.assertEqual(commit, plan.entries['go.mod'][2]['commit'])
        bad = copy.deepcopy(request)
        bad['overlay'][0]['origin'].update(commit=request['git']['base_commit'], tree=request['git']['base_tree'])
        with self.assertRaises(build.Failure):
            build.SourcePlan(provider, bad, build.Budget())
        staged = copy.deepcopy(request)
        staged['overlay'][0]['origin'].update(kind='staged_uncommitted', commit=None, tree=None,
                                             staged_capture_manifest_sha256='e'*64)
        result = build.SourcePlan(provider, staged, build.Budget())
        self.assertIsNone(result.entries['go.mod'][2]['commit'])
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve()
            cap = build.Directory.open_absolute(str(root))
            try:
                manifest = result.materialize(cap)
                self.assertEqual(raw, (root/'go.mod').read_bytes())
                self.assertEqual(build.source_digest(manifest['files']), build.check_source(cap, manifest, build.Budget()))
                (root/'go.mod').write_bytes(b'mutated')
                with self.assertRaises(build.Failure):
                    build.check_source(cap, manifest, build.Budget())
            finally:
                cap.close()

    def test_unsafe_tree_origin_delete_and_framing(self):
        provider, request = fixture_source()
        before = build.SourcePlan(provider, request, build.Budget())._binding(
            build.SourcePlan(provider, request, build.Budget()).entries['go.mod'])
        delete = {'op': 'delete', 'path': 'go.mod', 'before': before, 'after': None,
                  'origin': {'kind': 'staged_uncommitted', 'commit': None, 'tree': None,
                             'path': 'go.mod', 'blob': None, 'staged_capture_manifest_sha256': 'e'*64, 'role': None}}
        build.validate('overlay', delete)
        bad = copy.deepcopy(delete); bad['after'] = before
        with self.assertRaises(build.Failure):
            build.validate('overlay', bad)
        raw = b'100644 X\0'+b'x'*20+b'100644 x\0'+b'y'*20
        with self.assertRaises(build.Failure):
            build.parse_tree(raw)
        with self.assertRaises(build.Failure):
            build._source_path('docs/refactoring/index.md')
        entries = [{'path': 'go.mod', 'size': 1, 'mode': '100644', 'sha256': sha(b'x')}]
        modified = copy.deepcopy(entries); modified[0]['mode'] = '100755'
        self.assertNotEqual(build.source_digest(entries), build.source_digest(modified))


def fixture_zip(items=None):
    items = items or [('example.org/m@v1.0.0/go.mod', b'module example.org/m\n'),
                      ('example.org/m@v1.0.0/x.go', b'package m\n')]
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for name, raw in items:
            z.writestr(name, raw)
    return output.getvalue()


def fixture_seed(parent, include_ziphash=True):
    root = parent/'seed'; root.mkdir(mode=0o700)
    contents = {'mod': b'module example.org/m\n', 'zip': fixture_zip(),
                'info': b'{"Version":"v1.0.0","Time":"2026-10-18T00:00:00Z"}'}
    if include_ziphash:
        contents['ziphash'] = b'h1:caller-is-not-checksum-authority'
    entries = []
    for kind, raw in contents.items():
        relative = 'cache/download/example.org/m/@v/v1.0.0.'+kind
        p = root/relative; p.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        p.write_bytes(raw); p.chmod(0o600)
        entries.append({'path': relative, 'kind': kind, 'size': len(raw), 'sha256': sha(raw)})
    manifest = {'schema_version': 2, 'modules': [{'path': 'example.org/m', 'version': 'v1.0.0', 'files': entries}]}
    raw = build.canonical(manifest); path = parent/'seed.json'; path.write_bytes(raw); path.chmod(0o600)
    sums = {('example.org/m', 'v1.0.0/go.mod'): reference_h1({'go.mod': contents['mod']}),
            ('example.org/m', 'v1.0.0'): reference_h1({'example.org/m@v1.0.0/go.mod': contents['mod'], 'example.org/m@v1.0.0/x.go': b'package m\n'})}
    return {'root': str(root), 'manifest_path': str(path), 'manifest_sha256': sha(raw)}, sums, contents


class HashSeedControls(unittest.TestCase):
    def test_h1_primary_vector_and_zip_names_not_raw_zip_hash(self):
        files = {'hello.txt': b'hello world\n', 'sub/goodbye.txt': b'goodbye world\n'}
        self.assertEqual(reference_h1(files), build.h1_files({n: sha(v) for n, v in files.items()}))
        self.assertNotEqual(reference_h1(files), reference_h1({'renamed.txt': b'hello world\n', 'sub/goodbye.txt': b'goodbye world\n'}))
        with tempfile.TemporaryDirectory() as temp:
            p = pathlib.Path(temp).resolve()/'m.zip'; raw = fixture_zip(); p.write_bytes(raw)
            fd = os.open(p, os.O_RDONLY)
            try:
                actual, entries = build.zip_h1(fd, 'example.org/m', 'v1.0.0', build.Budget())
                self.assertEqual(reference_h1({'example.org/m@v1.0.0/go.mod': b'module example.org/m\n',
                                               'example.org/m@v1.0.0/x.go': b'package m\n'}), actual)
                self.assertNotEqual(actual[3:], base64.b64encode(hashlib.sha256(raw).digest()).decode())
                self.assertEqual(2, len(entries))
            finally:
                os.close(fd)

    def test_zip_paths_collisions_links_crc_and_forged_expansion(self):
        bad = [[('example.org/m@v1.0.0/../escape', b'x')],
               [('example.org/m@v1.0.0/X', b'x'), ('example.org/m@v1.0.0/x', b'y')],
               [('example.org/m@v1.0.0/go.mod', b'x'), ('example.org/m@v1.0.0/sub/go.mod', b'y')],
               [('other/m@v1.0.0/x', b'x')], [('example.org/m@v1.0.0/CON', b'x')]]
        raw = fixture_zip()
        forged = bytearray(raw); central = forged.index(b'PK\x01\x02')
        struct.pack_into('<I', forged, central+24, 1)  # false uncompressed size; raw deflate must expose it
        corrupt = bytearray(raw); corrupt[corrupt.index(b'PK\x01\x02')+16] ^= 1
        examples = [fixture_zip(items) for items in bad]+[bytes(forged), bytes(corrupt)]
        link = io.BytesIO()
        with zipfile.ZipFile(link, 'w') as archive:
            entry = zipfile.ZipInfo('example.org/m@v1.0.0/link'); entry.external_attr = (stat.S_IFLNK | 0o777)<<16
            archive.writestr(entry, b'target')
        examples.append(link.getvalue())
        with tempfile.TemporaryDirectory() as temp:
            p = pathlib.Path(temp).resolve()/'m.zip'
            for raw in examples:
                with self.subTest(digest=sha(raw)):
                    p.write_bytes(raw); fd = os.open(p, os.O_RDONLY)
                    try:
                        with self.assertRaises((build.Failure, zipfile.BadZipFile)):
                            build.zip_h1(fd, 'example.org/m', 'v1.0.0', build.Budget())
                    finally:
                        os.close(fd)

    def test_actual_go_sum_authority_two_independent_copies_and_generated_ziphash(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = pathlib.Path(temporary).resolve()
            spec, sums, contents = fixture_seed(parent, include_ziphash=False)
            seed = build.Seed(spec, sums, build.Budget())
            try:
                a = parent/'A'; b = parent/'B'; a.mkdir(); b.mkdir()
                with build.Directory.open_absolute(str(a)) as ca, build.Directory.open_absolute(str(b)) as cb:
                    seed.copy_to(ca); seed.copy_to(cb)
                    path = 'cache/download/example.org/m/@v/v1.0.0.ziphash'
                    self.assertEqual(sums[('example.org/m', 'v1.0.0')].encode(), (a/path).read_bytes())
                    self.assertNotEqual((a/path).stat().st_ino, (b/path).stat().st_ino)
                    build.verify_seed_destination(seed, ca, build.Budget())
                    (a/path).write_bytes(b'forged')
                    with self.assertRaises(build.Failure):
                        build.verify_seed_destination(seed, ca, build.Budget())
                wrong = dict(sums); wrong[('example.org/m', 'v1.0.0')] = 'h1:'+base64.b64encode(b'x'*32).decode()
                with self.assertRaisesRegex(build.Failure, 'GoSumMismatch'):
                    build.Seed(spec, wrong, build.Budget())
            finally:
                seed.close()

    def test_original_pinned_fd_not_reopened_replacement_and_inplace_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = pathlib.Path(temporary).resolve(); spec, sums, contents = fixture_seed(parent)
            seed = build.Seed(spec, sums, build.Budget())
            try:
                path = pathlib.Path(spec['root'])/'cache/download/example.org/m/@v/v1.0.0.mod'
                old = path.with_name('saved'); path.rename(old); path.write_bytes(b'forged')
                destination = parent/'copy'; destination.mkdir()
                with build.Directory.open_absolute(str(destination)) as cap:
                    seed.copy_to(cap)
                    self.assertEqual(contents['mod'], (destination/path.relative_to(spec['root'])).read_bytes())
                old.write_bytes(b'mutated-original')
                with self.assertRaises(build.Failure):
                    seed.recheck()
            finally:
                seed.close()

    def test_manifest_and_seed_membership_missing_extra_symlink_hardlink(self):
        for mode in ('missing', 'extra', 'symlink', 'hardlink', 'manifest'):
            with tempfile.TemporaryDirectory() as temporary:
                parent = pathlib.Path(temporary).resolve(); spec, sums, _ = fixture_seed(parent)
                p = pathlib.Path(spec['root'])/'cache/download/example.org/m/@v/v1.0.0.mod'
                if mode == 'missing': p.unlink()
                elif mode == 'extra': (pathlib.Path(spec['root'])/'unexpected').write_bytes(b'x')
                elif mode == 'symlink': p.rename(parent/'other'); p.symlink_to(parent/'other')
                elif mode == 'hardlink': os.link(p, parent/'other')
                else: pathlib.Path(spec['manifest_path']).write_bytes(b'{}')
                with self.subTest(mode=mode), self.assertRaises((build.Failure, OSError)):
                    build.Seed(spec, sums, build.Budget())


class FDControls(unittest.TestCase):
    def test_fifo_nonregular_symlink_and_hardlink_refused_without_wait(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); (root/'regular').write_bytes(b'x')
            os.mkfifo(root/'fifo', 0o600); (root/'link').symlink_to(root/'regular'); os.link(root/'regular', root/'hard')
            with build.Directory.open_absolute(str(root)) as cap:
                for name in ('fifo', 'link', 'hard'):
                    with self.subTest(name=name), self.assertRaises((build.Failure, OSError)):
                        cap.file(name)

    def test_real_destination_rehash_detects_write_corruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = pathlib.Path(temporary).resolve(); spec, sums, _ = fixture_seed(parent)
            seed = build.Seed(spec, sums, build.Budget()); destination = parent/'copy'; destination.mkdir()
            original = build._write_all; writes = [0]
            def corrupt(fd, data, budget):
                writes[0] += 1
                original(fd, b'X'*len(data) if writes[0] == 1 else data, budget)
            try:
                with build.Directory.open_absolute(str(destination)) as cap, mock.patch.object(build, '_write_all', corrupt):
                    with self.assertRaisesRegex(build.Failure, 'SeedMutation'):
                        seed.copy_to(cap)
            finally:
                seed.close()

    def test_retained_root_replacement_not_accepted_as_original(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = pathlib.Path(temporary).resolve(); root = parent/'root'; root.mkdir()
            with build.Directory.open_absolute(str(root)) as cap:
                root.rename(parent/'old'); root.mkdir()
                with self.assertRaisesRegex(build.Failure, 'InputMutation'):
                    cap.verify_name()

    def test_failure_constructor_closes_manifest_descriptor(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = pathlib.Path(temporary).resolve(); (parent/'m.json').write_bytes(b'{}')
            with build.Directory.open_absolute(str(parent)) as cap:
                opened = []; original = build.open_absolute_file
                def track(*args, **kwargs):
                    fd = original(*args, **kwargs); opened.append(fd); return fd
                with mock.patch.object(build, 'open_absolute_file', track):
                    with self.assertRaises(build.Failure):
                        build.Fingerprint(cap, {'manifest_path': str(parent/'m.json'), 'manifest_sha256': 'a'*64}, build.Budget())
                self.assertEqual(1, len(opened))
                with self.assertRaises(OSError): os.fstat(opened[0])


def regular_record(path):
    st = path.stat()
    return {'public_path': str(path), 'sha256': sha(path.read_bytes()), 'size': st.st_size,
            'identity': build.fd_identity(st)}


def link_record(path, target, links=None):
    steps = []
    for p in links or [path]:
        raw = os.readlink(os.fsencode(p))
        steps.append({'path': str(p), 'text_sha256': sha(raw), 'text_size': len(raw),
                      'identity': build.fd_identity(p.lstat())})
    return {'path': str(path), 'kind': 'symlink', 'regular': None,
            'link': {'steps': steps, 'resolved_regular': regular_record(target)}}


class NativeLinkControls(unittest.TestCase):
    def test_link_target_original_fd_recheck_retarget_and_regular_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); a = root/'a'; b = root/'b'; a.write_bytes(b'a'); b.write_bytes(b'b')
            link = root/'link'; link.symlink_to('a')
            with mock.patch.object(build, 'PUBLIC_NATIVE_PREFIXES', (str(root),)):
                cap = build.NativeFile(link_record(link, a), [str(root)], build.Budget())
                try:
                    cap.recheck(); link.unlink(); link.symlink_to('b')
                    with self.assertRaises(build.Failure): cap.recheck()
                finally: cap.close()
                record = {'path': str(a), 'kind': 'regular', 'regular': regular_record(a), 'link': None}
                cap = build.NativeFile(record, [str(root)], build.Budget())
                try:
                    a.rename(root/'old'); a.write_bytes(b'a')
                    with self.assertRaises(build.Failure): cap.recheck()
                finally: cap.close()

    def test_chains_cycle_escape_directory_and_text_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); a = root/'a'; a.write_bytes(b'a')
            first = root/'one'; second = root/'two'; first.symlink_to('two'); second.symlink_to('a')
            with mock.patch.object(build, 'PUBLIC_NATIVE_PREFIXES', (str(root),)):
                record = link_record(first, a, [first, second]); cap = build.NativeFile(record, [str(root)], build.Budget()); cap.close()
                bad = copy.deepcopy(record); bad['link']['steps'][0]['text_sha256'] = 'a'*64
                with self.assertRaises(build.Failure): build.NativeFile(bad, [str(root)], build.Budget())
                for target in ('one', '../outside', 'directory'):
                    second.unlink(); second.symlink_to(target)
                    if target == 'directory': (root/'directory').mkdir()
                    with self.subTest(target=target), self.assertRaises((build.Failure, OSError)):
                        build.NativeFile(record, [str(root)], build.Budget())
                directory = root/'directory'; (directory/'f').write_bytes(b'f')
                middle = root/'middle'; middle.symlink_to(directory, target_is_directory=True)
                bad = {'path': str(middle/'f'), 'kind': 'regular', 'regular': regular_record(directory/'f'), 'link': None}
                bad['regular']['public_path'] = bad['path']
                with self.assertRaises((build.Failure, OSError)): build.NativeFile(bad, [str(root)], build.Budget())

    def test_native_link_does_not_weaken_source_seed_or_toolchain_no_link(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); (root/'real').write_bytes(b'x'); (root/'link').symlink_to('real')
            with build.Directory.open_absolute(str(root)) as cap:
                with self.assertRaises(OSError): cap.file('link')
                with self.assertRaises((build.Failure, OSError)):
                    build.walk_regular(cap, build.Budget(), 100, 100, 100)


class EvidenceEnvironmentControls(unittest.TestCase):
    def test_owned_output_manifest_and_mutation_no_receipt_cycle(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = pathlib.Path(temporary).resolve(); store = build.OutputStore(str(parent/'output'), build.Budget())
            try:
                store.put('request.json', b'raw request', 'request')
                manifest = store.verified_manifest()
                self.assertEqual('corsa.t5g.v2.outputs\0'.encode()+build.canonical(manifest),
                                 b'corsa.t5g.v2.outputs\0'+build.canonical(manifest))
                self.assertEqual(sha(b'corsa.t5g.v2.outputs\0'+build.canonical(manifest)), build.domain_hash('outputs', manifest))
                bad = copy.deepcopy(manifest); bad['files'][0]['path'] = 'receipt.json'
                with self.assertRaises(build.Failure): build.validate('outputs', bad)
                fd = store.create('unclosed', 'metadata')
                with self.assertRaisesRegex(build.Failure, 'UnclosedEvidence'): store.verified_manifest()
                store.abandon('unclosed')
                (parent/'output/request.json').write_bytes(b'forged')
                with self.assertRaisesRegex(build.Failure, 'EvidenceMutation'): store.verified_manifest()
                with self.assertRaises(FileExistsError): build.OutputStore(str(parent/'output'), build.Budget())
            finally: store.root.close()

    def test_environment_does_not_inherit_bait_and_no_cgo_fallback(self):
        request = sample('request')
        class Label:
            def __init__(self, name): self.label = name
        lane = {key: Label('/private/'+key) for key in ('source', 'cache', 'modules', 'home', 'tmp', 'gopath', 'bin')}
        bait = {'CORSA_PRIVATE_KEY': 'never-capture', 'HTTPS_PROXY': 'secret', 'GOFLAGS': '-race',
                'GOTOOLCHAIN': 'auto', 'GOCACHEPROG': '/foreign', 'CC': '/wrapper'}
        with mock.patch.dict(os.environ, bait):
            env = build.environment_for_lane(request, lane, '/go', '/tools', 123)
        self.assertEqual('0', env['CGO_ENABLED']); self.assertEqual('off', env['GOPROXY'])
        self.assertEqual('off', env['GOSUMDB']); self.assertEqual('local', env['GOTOOLCHAIN'])
        self.assertEqual('123', env['SOURCE_DATE_EPOCH'])
        self.assertFalse(any(k in env for k in ('CORSA_PRIVATE_KEY', 'HTTPS_PROXY', 'GOCACHEPROG', 'CC')))
        request.update(flavour='native-linux-arm64', native=sample('native_request'))
        env = build.environment_for_lane(request, lane, '/go', '/tools', 123)
        self.assertEqual('1', env['CGO_ENABLED'])
        self.assertEqual('-O2 -g', env['CGO_CFLAGS'])

    def test_fixed_artifact_inspection_and_deferred_runtime(self):
        raw = (b'/private/bin: go1.27.3\n\tpath\tgithub.com/FastCorp/Corsa/cmd/corsa-node\n'
               b'\tmod\tgithub.com/FastCorp/Corsa\t(devel)\t\n'
               b'\tbuild\t-buildmode=exe\n\tbuild\t-compiler=gc\n\tbuild\t-trimpath=true\n'
               b'\tbuild\tCGO_ENABLED=0\n\tbuild\tGOARCH=arm64\n\tbuild\tGOOS=linux\n\tbuild\tGOARM64=v8.0\n')
        # Use the source-declared module path expected by the fixed target map.
        text = 'example.org/corsa'
        raw = raw.replace(b'github.com/FastCorp/Corsa', text.encode())
        parsed = build.parse_artifact_metadata(raw, b'build-id\n', 'corsa-node')
        self.assertEqual('0', parsed['settings']['CGO_ENABLED'])
        for bad in (raw.replace(b'go1.27.3', b'go1.27.2'), raw+b'\tbuild\tvcs.modified=false\n',
                    raw.replace(b'\tGOARCH=arm64', b'\tGOARCH=amd64')):
            with self.subTest(bad=sha(bad)), self.assertRaises(build.Failure):
                build.parse_artifact_metadata(bad, b'build-id\n', 'corsa-node')

    def test_unsupported_platform_never_launches_or_creates_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); request = root/'request.json'
            request.write_bytes(build.canonical(sample('request')))
            with mock.patch.object(build.platform, 'system', return_value='Darwin'), \
                 mock.patch.object(build.Executable, 'pin', side_effect=AssertionError('external discovery')), \
                 mock.patch.object(build.LinuxSupervisor, 'run_owned', side_effect=AssertionError('external command')), \
                 contextlib.redirect_stderr(io.StringIO()) as errors:
                self.assertEqual(2, build.main(['--request', str(request), '--output', str(root/'output')]))
            self.assertIn('UnsupportedSupervisor', errors.getvalue())
            self.assertFalse((root/'output').exists())

    def test_input_output_ancestor_or_alias_scope(self):
        request = sample('request')
        request['git']['directory'] = '/owned/git'; request['seed']['root'] = '/owned/seed'
        request['toolchain']['goroot'] = '/owned/toolchain'
        build.check_output_scope(request, '/different/output')
        for output in ('/owned', '/owned/seed', '/owned/seed/output', '/owned/toolchain/output'):
            with self.subTest(output=output), self.assertRaises(build.Failure):
                build.check_output_scope(request, output)


class FullNodeTargetControls(unittest.TestCase):
    def test_fullnode_request_is_fixed_and_three_target_bound_is_retained(self):
        document = json.loads(SCHEMA.read_text()); defs = document['$defs']
        request = sample('request')
        request['targets'] = ['fullnode', 'sdkedge', 'linuxhelper']
        build.validate('request', request)
        self.assertTrue(schema_validate(request, defs['request'], defs))
        self.assertEqual('./internal/testutil/loadstand/fullnode', build.TARGETS['fullnode'])
        self.assertEqual('./cmd/corsa-node', build.TARGETS['corsa-node'])
        for targets in (['./internal/testutil/loadstand/fullnode'], ['fullnode;other'],
                        ['unknown'], ['fullnode', 'sdkedge', 'linuxhelper', 'corsa-node']):
            bad = copy.deepcopy(request); bad['targets'] = targets
            with self.subTest(targets=targets):
                with self.assertRaises(build.Failure): build.validate('request', bad)
                self.assertFalse(schema_validate(bad, defs['request'], defs))

    def test_fullnode_metadata_and_receipt_bind_exact_target_not_node_alias(self):
        raw = (b'/private/bin: go1.27.3\n'
               b'\tpath\texample.org/corsa/internal/testutil/loadstand/fullnode\n'
               b'\tmod\texample.org/corsa\t(devel)\t\n'
               b'\tbuild\t-trimpath=true\n\tbuild\tCGO_ENABLED=1\n'
               b'\tbuild\tGOARCH=arm64\n\tbuild\tGOOS=linux\n\tbuild\tGOARM64=v8.0\n')
        parsed = build.parse_artifact_metadata(raw, b'build-id\n', 'fullnode')
        self.assertEqual('example.org/corsa/internal/testutil/loadstand/fullnode', parsed['path'])
        self.assertEqual('deferred_to_owned_runtime', parsed['runtime_exe_state'])
        for wrong in ('corsa-node', 'sdkedge', 'linuxhelper'):
            with self.subTest(wrong=wrong), self.assertRaises(build.Failure):
                build.parse_artifact_metadata(raw, b'build-id\n', wrong)
        receipt = sample('receipt'); targets = ['fullnode', 'sdkedge', 'linuxhelper']
        receipt['artifacts'] = []
        for n in (0, 1):
            receipt['lanes'][n]['targets'] = list(targets)
            for target in targets:
                artifact = sample('artifact'); artifact.update(lane=n, target=target, sha256=sha(target.encode()))
                receipt['artifacts'].append(artifact)
        build.validate('receipt', receipt)
        for case in ('missing', 'wrong-target', 'wrong-peer-lane-hash'):
            bad = copy.deepcopy(receipt)
            if case == 'missing': bad['artifacts'].pop()
            elif case == 'wrong-target': bad['artifacts'][0]['target'] = 'corsa-node'
            else: bad['artifacts'][3]['sha256'] = 'e'*64
            with self.subTest(case=case), self.assertRaises(build.Failure): build.validate('receipt', bad)

    def test_actual_lane_go_probe_uses_canonical_supported_version(self):
        self.assertEqual('go1.27.3', build.GO_VERSION)
        self.assertEqual('arm64', build.validate_go_probe(b'go version go1.27.3 linux/arm64\n'))
        for raw in (b'go version go1.27.2 linux/arm64\n',
                    b'go version go1.27.3 darwin/arm64\n',
                    b'go version go1.27.3 linux/arm64',
                    b'go version go1.27.3 linux/arm64\nextra\n'):
            with self.subTest(raw=raw), self.assertRaises(build.Failure): build.validate_go_probe(raw)
        tree = ast.parse(SCRIPT.read_text())
        lane = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == '_lane_body')
        calls = [n for n in ast.walk(lane) if isinstance(n, ast.Call) and
                 isinstance(n.func, ast.Name) and n.func.id == 'validate_go_probe']
        self.assertEqual(['version'], [ast.unparse(n.args[0]) for n in calls])
        self.assertEqual(build.GO_VERSION, build.empty_receipt(sample('request'), build.canonical(sample('request')))['environment']['go_version'])
        self.assertEqual([build.GO_VERSION], build.DEFS['environment']['go_version']['values'])

    def test_fullnode_build_argv_stays_on_original_owned_fixed_edge(self):
        tree = ast.parse(SCRIPT.read_text())
        lane = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == '_lane_body')
        argv = [n.value for n in ast.walk(lane) if isinstance(n, ast.Assign) and
                any(isinstance(x, ast.Name) and x.id == 'argv' for x in n.targets)]
        self.assertEqual(1, len(argv)); self.assertIsInstance(argv[0], ast.Tuple)
        self.assertEqual(['self.go.path', "'build'", "'-mod=readonly'", "'-trimpath'", "'-buildvcs=false'",
                          "'-ldflags'", "'-s -w'", "'-o'", 'output', 'TARGETS[target]'],
                         [ast.unparse(x) for x in argv[0].elts])
        # New fixed identity uses the same original package-closure and metadata
        # binding. No requested path, new external edge or arbitrary flag exists.


class ClosedLaunchControls(unittest.TestCase):
    def test_only_owned_launch_edges_no_external_fixture_switch(self):
        tree = ast.parse(SCRIPT.read_text())
        parents = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node): parents[child] = node
        def owner(node):
            while node in parents:
                node = parents[node]
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)): return node.name
        launches = []; fd_preflights = 0
        forbidden = {'system', 'popen', 'Popen', 'run', 'call', 'check_call', 'check_output',
                     'posix_spawn', 'posix_spawnp', 'spawnv', 'spawnve', 'execl', 'execvp', 'execvpe'}
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                self.assertFalse(any(x.name.split('.')[0] == 'subprocess' for x in node.names))
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == 'os':
                self.assertNotIn(node.attr, forbidden)
                if node.attr in ('fork', 'execve'):
                    if node.attr == 'execve' and owner(node) == 'preflight':
                        self.assertIsInstance(parents[node], ast.Compare)
                        self.assertEqual('os.supports_fd', ast.unparse(parents[node].comparators[0]))
                        fd_preflights += 1
                    else:
                        self.assertIsInstance(parents[node], ast.Call)  # no function-value escape
                        launches.append((node.attr, owner(node)))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                self.assertNotIn(node.func.id, ('eval', 'exec', '__import__'))
        self.assertEqual([('fork', 'run_owned'), ('execve', '_stub')], sorted(launches, reverse=True))
        self.assertEqual(1, fd_preflights)
        # Fixed target and build arguments are a closed reviewed production edge.
        self.assertEqual({'corsa-node': './cmd/corsa-node', 'sdkedge': './internal/testutil/loadstand/sdkedge',
                          'linuxhelper': './internal/testutil/loadstand/linuxhelper',
                          'fullnode': './internal/testutil/loadstand/fullnode'}, build.TARGETS)
        main = next(x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == 'main')
        arguments = [x.args[0].value for x in ast.walk(main) if isinstance(x, ast.Call) and
                     isinstance(x.func, ast.Attribute) and x.func.attr == 'add_argument']
        self.assertEqual(['--request', '--output'], arguments)
        self.assertFalse(any(isinstance(x, ast.Name) and x.id == 'FixtureObjects' for x in ast.walk(tree)))

    def test_directional_control_frames_partial_eof_and_extra(self):
        # Real nonblocking local pipes, no child/fork or tool execution.
        for payload, accepted in ((b'READY nonce\n', True), (b'READY wrong\n', False),
                                  (b'READY nonce\nextra', False), (b'READY ', False)):
            read, write = os.pipe()
            os.set_blocking(read, False); os.set_blocking(write, False)
            try:
                os.write(write, payload); os.close(write); write = None
                if accepted: build.LinuxSupervisor._receive(read, b'READY nonce\n', build.Budget(time.monotonic()+.2))
                else:
                    with self.assertRaises(build.Failure):
                        build.LinuxSupervisor._receive(read, b'READY nonce\n', build.Budget(time.monotonic()+.2))
            finally:
                os.close(read)
                if write is not None: os.close(write)


class HighFDControlWaitControls(unittest.TestCase):
    """Real own nonblocking pipes above FD_SETSIZE; no child or tool launch."""
    @contextlib.contextmanager
    def high_pipe(self, writer=False):
        import fcntl
        import resource
        limits = resource.getrlimit(resource.RLIMIT_NOFILE)
        wanted = max(limits[0], 4096)
        self.assertTrue(limits[1] == resource.RLIM_INFINITY or wanted <= limits[1],
                        'fixture needs a permitted soft descriptor limit4096')
        owned = []
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, limits[1]))
            read, write = os.pipe(); owned.extend((read, write))
            os.set_blocking(read, False); os.set_blocking(write, False)
            high = fcntl.fcntl(write if writer else read, fcntl.F_DUPFD, 2048)
            owned.append(high)
            os.set_inheritable(high, False)
            self.assertGreaterEqual(high, 2048)
            self.assertFalse(os.get_blocking(high))
            self.assertFalse(os.get_inheritable(high))
            yield read, write, high
        finally:
            try:
                for fd in reversed(owned): os.close(fd)
                for fd in owned:
                    with self.assertRaises(OSError) as closed: os.fstat(fd)
                    self.assertEqual(9, closed.exception.errno)
            finally:
                resource.setrlimit(resource.RLIMIT_NOFILE, limits)
            self.assertEqual(limits, resource.getrlimit(resource.RLIMIT_NOFILE))

    def test_high_fd_receive_wait_preserves_exact_original_frame(self):
        with self.high_pipe() as (_, write, high):
            original_poll = build.select.poll
            calls = []; frame = b'READY original-high-fd\n'
            test = self
            class Waiter:
                def __init__(self): self.actual = original_poll()
                def register(self, fd, events):
                    test.assertEqual((high, build.select.POLLIN), (fd, events))
                    self.actual.register(fd, events)
                def poll(self, milliseconds):
                    test.assertGreaterEqual(milliseconds, 0); test.assertLessEqual(milliseconds, 100)
                    calls.append(milliseconds)
                    test.assertEqual(len(frame), os.write(write, frame))
                    result = self.actual.poll(milliseconds)
                    test.assertTrue(any(fd == high and events & build.select.POLLIN for fd, events in result))
                    return result
            with mock.patch.object(build.select, 'poll', side_effect=Waiter):
                build.LinuxSupervisor._receive(high, frame, build.Budget(time.monotonic()+1))
            self.assertEqual(1, len(calls))

    def test_high_fd_send_wait_preserves_exact_original_frame(self):
        with self.high_pipe(writer=True) as (read, _, high):
            filled = 0
            while filled <= 1024*1024:
                try: filled += os.write(high, b'x'*4096)
                except BlockingIOError: break
            self.assertGreater(filled, 0); self.assertLessEqual(filled, 1024*1024)
            original_poll = build.select.poll
            calls = []; frame = b'GO original-high-fd\n'
            test = self
            class Waiter:
                def __init__(self): self.actual = original_poll()
                def register(self, fd, events):
                    test.assertEqual((high, build.select.POLLOUT), (fd, events))
                    self.actual.register(fd, events)
                def poll(self, milliseconds):
                    test.assertGreaterEqual(milliseconds, 0); test.assertLessEqual(milliseconds, 100)
                    calls.append(milliseconds)
                    left = filled
                    while left:
                        part = os.read(read, min(65536, left))
                        test.assertTrue(part); test.assertEqual(b'x'*len(part), part)
                        left -= len(part)
                    result = self.actual.poll(milliseconds)
                    test.assertTrue(any(fd == high and events & build.select.POLLOUT for fd, events in result))
                    return result
            with mock.patch.object(build.select, 'poll', side_effect=Waiter):
                build.LinuxSupervisor._send(high, frame, build.Budget(time.monotonic()+1))
            self.assertEqual(1, len(calls))
            self.assertEqual(frame, os.read(read, len(frame)+1))

    def test_known_control_wait_valueerrors_preserve_original_cause(self):
        for boundary in ('constructor', 'register', 'poll'):
            with self.subTest(boundary=boundary), self.high_pipe() as (_, _, high):
                cause = ValueError('fixture control-wait '+boundary)
                class Waiter:
                    def register(self, *_):
                        if boundary == 'register': raise cause
                    def poll(self, *_): raise cause
                factory = mock.Mock(side_effect=cause) if boundary == 'constructor' else Waiter
                with mock.patch.object(build.select, 'poll', side_effect=factory):
                    with self.assertRaisesRegex(build.Failure, '^ControlWaitInvalid$') as observed:
                        build.LinuxSupervisor._wait_control(high, build.select.POLLIN,
                                                           build.Budget(time.monotonic()+1))
                self.assertIs(cause, observed.exception.__cause__)
                self.assertTrue(observed.exception.incomplete)
                self.assertEqual('ready', observed.exception.phase)


@unittest.skipUnless(platform.system() == 'Linux', 'PENDING: actual Linux kernel supervisor controls; Darwin contract fixtures are not Linux PASS')
class LinuxSupervisorControls(unittest.TestCase):
    """Future bounded own-child controls; no Go/Git/compiler/new artifact launch.

    Invoke this class only in the separately authorized qualified Linux runtime.
    Internal test argv is fixed source, never an executable BuildRequest field.
    """
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name).resolve()
        self.budget = build.Budget(time.monotonic()+20)
        self.store = build.OutputStore(str(self.root/'out'), self.budget)
        self.supervisor = build.LinuxSupervisor(self.store, self.budget)
        path = pathlib.Path(sys.executable).resolve()
        self.exe = build.Executable.pin({'path': str(path), 'sha256': sha(path.read_bytes())}, self.budget)
        self.cwd = build.Directory.open_absolute(str(self.root))

    def tearDown(self):
        # Never delete fixture paths possibly held by unconfirmed writers.
        self.exe.close(); self.cwd.close(); self.store.root.close()
        if self.supervisor.anchor is None and not self.supervisor.poisoned:
            self.temp.cleanup()
        else:
            self.temp._finalizer.detach()

    def run_code(self, source, seconds=5):
        return self.supervisor.run_owned((self.exe.path, '-c', 'exec('+repr(source)+')'), self.exe,
                    self.cwd, {'PATH': '/usr/bin:/bin', 'PYTHONDONTWRITEBYTECODE': '1'}, seconds)

    def test_regular_outputs_all_children_joined_and_no_competing_wait(self):
        result = self.run_code("import os; print('owned'); os.write(2,b'diagnostic\\n')")
        self.assertTrue(result.joined); self.assertEqual(0, result.exit_code); self.assertIsNone(result.failure)
        self.assertEqual([], build.LinuxSupervisor.children()); self.assertIsNone(self.supervisor.anchor)
        self.assertTrue(stat.S_ISREG((self.root/'out'/result.stdout).stat().st_mode))
        self.assertEqual(b'owned\n', (self.root/'out'/result.stdout).read_bytes())
        # A sealed immutable return value is not backed by a mutable process/error.
        self.assertIsInstance(result.argv, tuple)
        with self.assertRaises(dataclasses.FrozenInstanceError): result.joined = False

    def test_leader_exit_descendant_inherited_regular_fd_term_kill_and_anchor_last(self):
        source = ("import os,signal,time\np=os.fork()\nif p==0:\n signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
                  " os.write(1,b'child-ready\\n')\n while True: time.sleep(.1)\nelse:\n time.sleep(.1)\n os._exit(0)\n")
        wait = os.waitpid; collected = []; signals = []; anchors = []; killpg = os.killpg
        def observe_wait(pid, flags):
            result = wait(pid, flags); collected.append(pid); return result
        def observe_signal(pid, sig):
            self.assertIsNotNone(self.supervisor.anchor)
            self.assertEqual(pid, self.supervisor.anchor)
            signals.append(sig); anchors.append(pid); return killpg(pid, sig)
        with mock.patch.object(build.os, 'waitpid', observe_wait), mock.patch.object(build.os, 'killpg', observe_signal):
            result = self.run_code(source)
        self.assertTrue(result.joined); self.assertEqual('DescendantsAfterLeader', result.failure)
        self.assertEqual([signal.SIGTERM, signal.SIGKILL], signals)
        self.assertGreaterEqual(len(collected), 2); self.assertEqual(anchors[0], collected[-1]); self.assertEqual([], build.LinuxSupervisor.children())
        self.assertEqual(0, result.exit_code)

    def test_before_ready_cancel_never_signals_guessed_group_or_execs_tool(self):
        def held_stub(*_):
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            while True: time.sleep(.05)
        def timeout(*_):
            raise build.Failure('Cancelled', 'ready', True)
        with mock.patch.object(build.LinuxSupervisor, '_stub', held_stub), \
             mock.patch.object(build.LinuxSupervisor, '_receive', timeout), \
             mock.patch.object(build.os, 'killpg', side_effect=AssertionError('unvalidated group')):
            result = self.run_code("open('must-not-exist','w').write('bad')")
        self.assertTrue(result.joined); self.assertEqual('Cancelled', result.failure)
        self.assertFalse((self.root/'must-not-exist').exists())
        self.assertEqual(('SIGTERM', 'sent'), result.signals[0])

    def test_control_wait_construction_failure_joins_original_pre_go_owner(self):
        def held_stub(*_):
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            while True: time.sleep(.05)
        cause = ValueError('fixture poll construction')
        with mock.patch.object(build.LinuxSupervisor, '_stub', staticmethod(held_stub)), \
             mock.patch.object(build.select, 'poll', side_effect=cause), \
             mock.patch.object(build.os, 'killpg', side_effect=AssertionError('unvalidated group')):
            result = self.run_code("open('must-not-exec','w').write('bad')")
        self.assertTrue(result.joined); self.assertEqual('ControlWaitInvalid', result.failure)
        self.assertFalse((self.root/'must-not-exec').exists())
        self.assertEqual(('SIGTERM', 'sent'), result.signals[0])
        self.assertEqual([], build.LinuxSupervisor.children())
        self.assertIsNone(self.supervisor.anchor); self.assertFalse(self.supervisor.poisoned)
        self.assertEqual({}, self.store.open_files)
        self.assertEqual(1, len(self.supervisor.records))
        self.assertEqual(result, self.supervisor.records[0])
        self.assertTrue((self.root/'out'/result.stdout).is_file())
        self.assertTrue((self.root/'out'/result.stderr).is_file())

    def test_valid_ready_with_wrong_kernel_group_refuses_go_and_never_guesses_group(self):
        # Real child/nonce/control pipes, but deliberately NO setsid/setpgid.
        # This test-local stub never executes the requested Python/tool argv.
        evidence_name, marker_name = 'wrong-kernel-group.json', 'must-not-exist'

        def wrong_group_stub(executable, argv, environment, cwd_fd, stdout_fd, stderr_fd,
                             ready_write, go_read, ready_read, go_write, nonce, deadline):
            try:
                os.close(ready_read); os.close(go_write)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                actual = build.proc_stat(os.getpid())
                identity = {key: actual[key] for key in ('pid', 'ppid', 'pgid', 'sid', 'start')}

                def write_private(name, raw):
                    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 0o600, dir_fd=cwd_fd)
                    try: build._write_all(fd, raw, build.Budget(deadline))
                    finally: os.close(fd)

                write_private(evidence_name, build.canonical(identity))
                build.LinuxSupervisor._send(ready_write, b'READY '+nonce+b'\n', build.Budget(deadline))
                try:
                    build.LinuxSupervisor._receive(go_read, b'GO '+nonce+b'\n', build.Budget(deadline))
                except build.Failure as failure:
                    if failure.code != 'ControlEOF': raise
                    # EOF is not voluntary completion: remain a real live child
                    # until retained direct-PID TERM (or qualified outer cleanup).
                    while True: signal.pause()
                write_private(marker_name, b'actual-GO-received\n')
                os.close(ready_write); os.close(go_read)
                os._exit(0)
            except BaseException:
                os._exit(126)  # fixture setup/control failure must NOT be a RED

        def read_identity():
            fd = self.cwd.file(evidence_name)
            try: raw = build.read_fd(fd, 512, build.Budget(time.monotonic()+1), private=True)
            finally: os.close(fd)
            identity = build.decode_json(raw, 512)
            if type(identity) is not dict or set(identity) != {'pid', 'ppid', 'pgid', 'sid', 'start'} or any(type(x) is not int or x <= 0 for x in identity.values()):
                raise RuntimeError('invalid private kernel identity evidence')
            return identity

        identity = None
        with mock.patch.object(build.os, 'killpg', side_effect=AssertionError('guessed group signal forbidden')):
            try:
                with mock.patch.object(build.LinuxSupervisor, '_stub', staticmethod(wrong_group_stub)):
                    result = self.run_code('pass')
                # MUST remain the FIRST post-run assertion: M3 only removes the
                # actual READY kernel-identity predicate and therefore writes GO.
                self.assertFalse((self.root/marker_name).exists())
                identity = read_identity()
                self.assertEqual(os.getpid(), identity['ppid'])
                self.assertNotEqual(identity['pid'], identity['pgid'])
                self.assertNotEqual(identity['pid'], identity['sid'])
                self.assertTrue(result.joined)
                self.assertEqual('GroupIdentityUnknown', result.failure)
                self.assertEqual(signal.SIGTERM, result.term_signal)
                self.assertEqual((('SIGTERM', 'sent'),), result.signals)
                self.assertIsNone(self.supervisor.anchor)
                self.assertEqual([], build.LinuxSupervisor.children())
            finally:
                pid = self.supervisor.anchor
                if pid is not None:
                    # Only an actual retained unreaped OWN direct child with the
                    # original recorded start incarnation is eligible for cleanup.
                    # Missing/changed evidence is ERROR/HOLD, never a guessed kill.
                    identity = identity if identity is not None else read_identity()
                    current = build.proc_stat(pid)
                    if identity['pid'] != pid or current['ppid'] != os.getpid() or identity['ppid'] != os.getpid() or current['start'] != identity['start'] or pid not in build.LinuxSupervisor.children():
                        raise RuntimeError('fixture child cleanup ownership unconfirmed')
                    os.kill(pid, signal.SIGKILL)
                    cleanup_end = time.monotonic()+3
                    while True:
                        exited = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                        if exited is not None:
                            if exited.si_pid != pid or os.waitpid(pid, 0)[0] != pid:
                                raise RuntimeError('fixture child reap identity changed')
                            self.supervisor.anchor = None
                            break
                        if time.monotonic() >= cleanup_end:
                            raise RuntimeError('fixture child cleanup unconfirmed')
                        time.sleep(.01)
                    # Do not rewrite result.joined/failure or clear poisoned state.

    def test_wrong_ready_nonce_refuses_go_before_tool_marker(self):
        original = build.LinuxSupervisor._send
        def wrong(fd, data, budget):
            if data.startswith(b'READY '): data = b'READY wrong\n'
            return original(fd, data, budget)
        with mock.patch.object(build.LinuxSupervisor, '_send', wrong):
            result = self.run_code("open('must-not-exist','w').write('bad')")
        self.assertTrue(result.joined); self.assertIsNotNone(result.failure)
        self.assertFalse((self.root/'must-not-exist').exists())

    def test_output_limit_and_deadline_terminate_and_join(self):
        with mock.patch.dict(build.LIMITS, {'stdout': 1024}):
            result = self.run_code("import os,time; os.write(1,b'x'*2048); time.sleep(10)")
        self.assertTrue(result.joined); self.assertEqual('OutputLimit', result.failure)
        self.assertGreater((self.root/'out'/result.stdout).stat().st_size, 1024)
        result = self.run_code('import time; time.sleep(10)', seconds=1)
        self.assertTrue(result.joined); self.assertEqual('CommandTimeout', result.failure)

    def test_unknown_membership_poison_retains_paths_and_refuses_next_command(self):
        with mock.patch.object(self.supervisor, '_members', side_effect=build.Failure('MembershipUnknown', 'supervisor', True)), \
             mock.patch.dict(build.LIMITS, {'term_seconds': 0, 'kill_seconds': 0}):
            result = self.run_code('import time; time.sleep(10)')
        self.assertFalse(result.joined); self.assertTrue(self.supervisor.poisoned)
        self.assertTrue((self.root/'out'/result.stdout).exists())
        with self.assertRaisesRegex(build.Failure, 'SupervisorPoisoned'): self.run_code('pass')
        # Fixture-only cleanup of the exact still-unreaped OWN anchor; production
        # correctly does not guess membership or advertise this as a joined result.
        pid = self.supervisor.anchor
        try: os.kill(pid, signal.SIGKILL)
        except ProcessLookupError: pass
        os.waitpid(pid, 0); self.supervisor.anchor = None; self.supervisor.poisoned = False



class AdditionalBoundaryControls(unittest.TestCase):
    def test_schema_runtime_structural_state_discriminants(self):
        defs = json.loads(SCHEMA.read_text())['$defs']
        examples = []
        value = sample('overlay'); value['before'] = sample('blob'); examples.append(('overlay', value))
        value = sample('lane'); value['source_before'] = 'a'*64; examples.append(('lane', value))
        value = sample('receipt')['lanes'][0]; value['joined'] = False; examples.append(('lane', value))
        value = sample('command'); value.update(joined=True, exit_code=None, term_signal=None); examples.append(('command', value))
        value = sample('command'); value.update(joined=False, exit_code=0, failure='unknown'); examples.append(('command', value))
        value = sample('image'); value['digest'] = 'a'*64; examples.append(('image', value))
        value = sample('receipt'); value['environment']['cgo'] = 1; examples.append(('receipt', value))
        value = sample('receipt'); value['cleanup']['possible_writers'] = True; examples.append(('receipt', value))
        for name, value in examples:
            with self.subTest(name=name, value=value):
                self.assertFalse(schema_validate(value, defs[name], defs))
                with self.assertRaises(build.Failure): build.validate(name, value)

    def test_zip_actual_count_bounded_before_stdlib_allocates_inventory(self):
        raw = bytearray(fixture_zip())
        end = raw.rfind(b'PK\x05\x06')
        struct.pack_into('<2H', raw, end+8, 1, 1)  # claim one while actual central has two
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary).resolve()/'m.zip'; path.write_bytes(raw)
            fd = os.open(path, os.O_RDONLY)
            try:
                with mock.patch.object(build.zipfile, 'ZipFile', side_effect=AssertionError('unbounded allocation reached')):
                    with self.assertRaises(build.Failure): build.zip_h1(fd, 'example.org/m', 'v1.0.0', build.Budget())
            finally: os.close(fd)

    def test_source_domain_has_one_framed_algorithm_and_no_empty_source(self):
        files = [{'path': 'go.mod', 'mode': '100644', 'sha256': sha(b'x'), 'size': 1}]
        path = b'go.mod'
        framed = b'corsa.t5g.v2.source\0'+len(path).to_bytes(8, 'big')+path+b'100644'+(1).to_bytes(8, 'big')+hashlib.sha256(b'x').digest()
        self.assertEqual(sha(framed), build.domain_hash('source', files))
        with self.assertRaises(build.Failure): build.domain_hash('source', [])

    def test_native_32_file_link_hop_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); target = root/'target'; target.write_bytes(b'x')
            links = [root/('l'+str(i)) for i in range(33)]
            for i, path in enumerate(links): path.symlink_to(links[i+1].name if i+1 < len(links) else target.name)
            with mock.patch.object(build, 'PUBLIC_NATIVE_PREFIXES', (str(root),)):
                record = link_record(links[1], target, links[1:]); cap = build.NativeFile(record, [str(root)], build.Budget()); cap.close()
                # The externally supplied record itself cannot declare >32 hops.
                record = link_record(links[0], target, links)
                with self.assertRaises(build.Failure): build.NativeFile(record, [str(root)], build.Budget())



class FailureLifecycleControls(unittest.TestCase):
    def test_schema_interpreter_ref_cycle_is_bounded(self):
        with self.assertRaisesRegex(AssertionError, 'bounded'):
            schema_validate({}, {'$ref': '#/$defs/cycle'}, {'cycle': {'$ref': '#/$defs/cycle'}})
        with self.assertRaisesRegex(AssertionError, 'external'):
            schema_validate({}, {'$ref': 'https://invalid.test/schema'}, {})

    def test_cancelled_output_write_closes_every_open_fd(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); budget = build.Budget()
            store = build.OutputStore(str(root/'out'), budget)
            fd = store.create('unfinished', 'metadata'); budget.cancelled[0] = True
            with self.assertRaisesRegex(build.Failure, 'Cancelled'):
                build._write_all(fd, b'never-completed', budget)
            store.close(); store.close()
            with self.assertRaises(OSError): os.fstat(fd)
            self.assertFalse((root/'out/receipt.json').exists())

    def test_partial_lane_capabilities_owned_before_later_open_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); registry = []; original = build.Directory.directory
            def refuse(cap, name, create=False):
                if name == 'modules': raise OSError('fixture open refusal')
                return original(cap, name, create)
            with build.Directory.open_absolute(str(root)) as cap:
                try:
                    with mock.patch.object(build.Directory, 'directory', refuse):
                        with self.assertRaises(OSError): build.new_lane(cap, 'A', registry)
                    self.assertEqual(1, len(registry)); self.assertEqual({'root', 'source'}, set(registry[0]))
                    build.safe_delete_contents(registry[0]['root'], build.Budget())
                finally:
                    for lane in registry:
                        for item in lane.values(): item.close()

    def test_link_text_must_be_utf8_bytes_not_replacement_decoding(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); target = root/'target'; target.write_bytes(b'x')
            link = root/'link'; os.symlink(b'\xff', os.fsencode(link))
            record = link_record(link, target)
            with mock.patch.object(build, 'PUBLIC_NATIVE_PREFIXES', (str(root),)):
                with self.assertRaisesRegex(build.Failure, 'NativeLinkEncoding'):
                    build.NativeFile(record, [str(root)], build.Budget())



class GitEvidenceRetentionControls(unittest.TestCase):
    def test_verified_transfer_keeps_raw_bytes_and_command_reference(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); store = build.OutputStore(str(root/'out'), build.Budget())
            raw = b'module example.org/fixture\n'; oid = build.git_oid('blob', raw)
            class Supervisor:
                def __init__(self): self.store = store
                def run_owned(self, *_):
                    store.put('commands/000001.stdout', raw, 'command_stdout')
                    store.put('commands/000001.stderr', b'', 'command_stderr')
                    result = build.CommandResult(1, ('/not-executed/git',), 'commands/000001.stdout', 'commands/000001.stderr', 0, None, True, None, ())
                    store.json('commands/000001.json', result.as_json(), 'command_record')
                    return result
            class Label: path = '/not-executed/git'; label = str(root)
            provider = build.GitObjects.__new__(build.GitObjects)
            provider.supervisor, provider.executable, provider.directory = Supervisor(), Label(), Label()
            provider.cwd, provider.environment, provider.budget = Label(), {}, build.Budget()
            try:
                self.assertEqual(raw, provider.object(oid, 'blob'))
                manifest = store.verified_manifest()
                self.assertIn('commands/000001.stdout', {x['path'] for x in manifest['files']})
                self.assertEqual(raw, (root/'out/commands/000001.stdout').read_bytes())
            finally: store.close()



class NativeFlagControls(unittest.TestCase):
    def test_declared_scopes_and_finite_non_input_flags(self):
        good = ['-I/source/headers', '-I', '/module/include', '-L/usr/scoped/lib', '-DNAME=1', '-pthread', '-O2', '-g', '-fPIC', '-lm', '-std=c11']
        build.check_native_flags(good, '/source', '/module', ['/usr/scoped'])
        build.check_native_flags(['-Irelative'], '/source', '/module', ['/usr/scoped'])
        for flags in (['-I/usr/undeclared'], ['-L/foreign'], ['-include', '/foreign/header'], ['@/foreign/response'],
                      ['-fplugin=/foreign/plugin'], ['-B/foreign'], ['-specs=/foreign'], ['-Wl,@/foreign'],
                      ['-I'], ['-isystem', '-invalid'], ['-I/foreign/../undeclared'], ['--sysroot=/foreign']):
            with self.subTest(flags=flags), self.assertRaises(build.Failure):
                build.check_native_flags(flags, '/source', '/module', ['/usr/scoped'])



class PinnedGoUnicodeControls(unittest.TestCase):
    def test_go_simple_fold_is_not_python_full_casefold(self):
        self.assertEqual('17.0.0', build.GO_UNICODE_VERSION)
        self.assertEqual('ac0e5d5d1b58ed4b02d7cb4a0085bdefbdb4f7aae6fadddbc9477f8463da8bd8', build.GO_UNICODE_TABLES_SHA256)
        self.assertEqual((647, 336, 94), (len(build.GO_LETTER_RANGES), len(build.GO_CASE_RANGES), len(build.GO_CASE_ORBIT)))
        for text, expected in (('KKk', 'kkk'), ('Sſs', 'sss'), ('ßẞ', 'ßß'), ('SS', 'ss'), ('İiıI', 'İiıi'), ('Σσς', 'ΣΣΣ')):
            with self.subTest(text=text): self.assertEqual(expected, build.go_path_fold(text))
        self.assertNotEqual(build.go_path_fold('ß'), build.go_path_fold('SS'))
        self.assertEqual(build.go_path_fold('ß'), build.go_path_fold('ẞ'))

    def test_newer_go_letter_and_non_equivalent_unicode_zip_names(self):
        letter = chr(0x11F02)  # Kawi letter from newer Unicode than host Python3.9.
        self.assertTrue(build.go_is_letter(ord(letter)))
        self.assertEqual(letter+'.go', build.module_file_path(letter+'.go'))
        self.assertFalse(build.go_is_letter(0x030A))
        for pair, accepted in ((('ß.go', 'SS.go'), True), (('ß.go', 'ẞ.go'), False), (('K.go', 'K.go'), False), ((letter+'.go', 'x.go'), True)):
            files = [('example.org/m@v1.0.0/'+name, b'package m') for name in pair]
            with tempfile.TemporaryDirectory() as temporary:
                path = pathlib.Path(temporary).resolve()/'m.zip'; path.write_bytes(fixture_zip(files)); fd = os.open(path, os.O_RDONLY)
                try:
                    if accepted:
                        actual, _ = build.zip_h1(fd, 'example.org/m', 'v1.0.0', build.Budget())
                        self.assertEqual(reference_h1(dict(files)), actual)
                    else:
                        with self.assertRaisesRegex(build.Failure, 'ModuleZipCollision'):
                            build.zip_h1(fd, 'example.org/m', 'v1.0.0', build.Budget())
                finally: os.close(fd)



class ReceiptAuthorityBindingControls(unittest.TestCase):
    def test_verified_input_evidence_and_equal_artifact_size_are_bound(self):
        for field in ('toolchain', 'offline', 'artifact_size'):
            value = sample('receipt')
            if field == 'artifact_size': value['artifacts'][1]['size'] += 1
            else: value[field]['sha256'] = 'b'*64
            with self.subTest(field=field), self.assertRaises(build.Failure): build.validate('receipt', value)



class ComparatorFixtureControls(unittest.TestCase):
    """Pure fake lanes exercise orchestration/actual stored bytes, never Go proof."""
    def fixture(self, store, mismatch=False, unconfirmed=False):
        request = sample('request'); raw = build.canonical(request)
        class Pair(build.BuildPair):
            def _prepare(pair):
                class Supervisor:
                    poisoned = False; anchor = None; records = (); cleanup_end = None
                class Seed:
                    manifest = {'schema_version': 2, 'modules': []}
                pair.supervisor, pair.seed = Supervisor(), Seed()
                for key in ('source', 'toolchain'): pair.receipt[key] = sample('evidence')
                pair.receipt['native'] = build.evidence('unavailable', reason='CompileOnly_CGO0')
                store.put('request.json', raw, 'request')
                for role in ('source', 'seed', 'toolchain', 'graph', 'closure', 'module_checks', 'metadata',
                             'lane_check', 'command_stdout', 'command_stderr', 'command_record', 'checkpoint'):
                    store.put('fixture/'+role, b'fixture-only-not-Go-evidence', role)
                pair.calls = []
            def _lane(pair, number):
                pair.calls.append(number)
                if unconfirmed:
                    pair.supervisor.poisoned = True
                    raise build.Failure('CleanupUnconfirmed', 'fixture_only', True)
                record = copy.deepcopy(sample('receipt')['lanes'][number])
                digest = build.domain_hash('seed', pair.seed.manifest)
                record.update(seed_before=digest, seed_after=digest)
                pair.receipt['lanes'][number] = record
                for target in request['targets']:
                    payload = b'fake-artifact-'+(b'different' if mismatch and number == 1 else b'equal')
                    actual = store.put('artifacts/'+str(number)+'-'+target, payload, 'artifact')
                    pair.receipt['artifacts'].append({'lane': number, 'target': target, 'size': actual['size'],
                        'sha256': actual['sha256'], 'reported_version_state': 'deferred_to_owned_runtime',
                        'runtime_exe_state': 'deferred_to_owned_runtime'})
        return Pair(request, raw, store, build.Budget())

    def test_fixture_comparator_equal_mismatch_and_unconfirmed_no_B(self):
        for mismatch, unconfirmed, status in ((False, False, 'verified_equal'), (True, False, 'failed'), (False, True, 'incomplete')):
            with tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary).resolve(); store = build.OutputStore(str(root/'out'), build.Budget())
                pair = self.fixture(store, mismatch, unconfirmed); published = []
                def fixture_publish(value):
                    self.assertEqual({}, store.open_files)
                    manifest = build.decode_json((root/'out/artifacts-manifest.json').read_bytes(), build.LIMITS['manifest_json'])
                    self.assertEqual(build.domain_hash('outputs', manifest), value['output_manifest_sha256'])
                    published.append(build.decode_json(build.canonical(value)))
                try:
                    with mock.patch.object(store, 'publish_receipt', fixture_publish): result = pair.execute()
                    self.assertEqual(status, result['status']); self.assertEqual(1, len(published))
                    self.assertEqual([0] if unconfirmed else [0, 1], pair.calls)
                    if unconfirmed:
                        self.assertTrue(result['cleanup']['possible_writers'])
                    elif mismatch:
                        self.assertEqual('different', result['comparison'])
                        self.assertEqual('ReproducibilityMismatch', result['failure']['code'])
                    else:
                        self.assertEqual('equal', result['comparison'])
                        self.assertFalse((root/'out/receipt.json').exists())  # publication mocked, no durable receipt claim
                finally: pair.close(); store.close()



class ExactCLIControls(unittest.TestCase):
    def test_abbreviation_equals_duplicate_and_unknown_switch_refused(self):
        for arguments in (['--req', '/x', '--output', '/y'], ['--request=/x', '--output=/y'],
                          ['--request', '/x', '--request', '/z', '--output', '/y'],
                          ['--fixture', '/x', '--output', '/y']):
            with self.subTest(arguments=arguments), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as result:
                build.main(arguments)
            self.assertEqual(2, result.exception.code)



class InterruptedObservationControls(unittest.TestCase):
    def test_generator_hash_failure_closes_just_opened_original_fd(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); store = build.OutputStore(str(root/'out'), build.Budget())
            request = sample('request'); pair = build.BuildPair(request, build.canonical(request), store, build.Budget())
            opened = []; original = build.open_absolute_file
            def track(*args, **kwargs):
                fd = original(*args, **kwargs); opened.append(fd); return fd
            try:
                with mock.patch.object(build, 'open_absolute_file', track), \
                     mock.patch.object(build, 'hash_fd', side_effect=build.Failure('FixtureInterruptedRead')):
                    with self.assertRaises(build.Failure): pair._generator_identity()
                self.assertEqual(1, len(opened))
                with self.assertRaises(OSError): os.fstat(opened[0])
            finally: pair.close(); store.close()

    def test_native_second_lstat_failure_closes_unregistered_parent(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve(); target = root/'target'; target.write_bytes(b'x'); link = root/'link'; link.symlink_to('target')
            record = link_record(link, target); original_stat = os.stat; original_open = build.Directory.open_absolute; calls = [0]; opened = []
            def changing(name, *args, **kwargs):
                if name == 'link' and kwargs.get('dir_fd') is not None:
                    calls[0] += 1
                    if calls[0] == 2: raise OSError('fixture concurrent unlink')
                return original_stat(name, *args, **kwargs)
            def track(path):
                cap = original_open(path); opened.append(cap.fd); return cap
            with mock.patch.object(build, 'PUBLIC_NATIVE_PREFIXES', (str(root),)), \
                 mock.patch.object(build.Directory, 'open_absolute', track), mock.patch.object(build.os, 'stat', changing):
                with self.assertRaises(OSError): build.NativeFile(record, [str(root)], build.Budget())
            self.assertEqual(1, len(opened))
            with self.assertRaises(OSError): os.fstat(opened[0])



class LaneTelemetryControls(unittest.TestCase):
    """Own temporary file/FD controls; no Go/Git/child/compiler execution."""
    def test_fresh_modes_are_separate_private_exact_and_environment_bound(self):
        with tempfile.TemporaryDirectory() as temporary:
            with build.Directory.open_absolute(str(pathlib.Path(temporary).resolve())) as root:
                lanes=[]
                try:
                    identities=[]
                    for name in ('A','B'):
                        lane=build.new_lane(root,name,lanes)
                        expected=build.initialize_lane_telemetry(lane['home'],build.Budget())
                        build.verify_lane_telemetry(lane['home'],expected,build.Budget())
                        mode=pathlib.Path(lane['home'].label)/'go/telemetry/mode'
                        self.assertEqual(b'off\n',mode.read_bytes())
                        st=mode.stat(); identities.append((st.st_dev,st.st_ino))
                        self.assertEqual(os.getuid(),st.st_uid)
                        self.assertEqual(0o600,stat.S_IMODE(st.st_mode)); self.assertEqual(1,st.st_nlink)
                        env=build.environment_for_lane(sample('request'),lane,'/go','/tools',123)
                        self.assertEqual(lane['home'].label,env['HOME'])
                        self.assertEqual(env['HOME'],env['XDG_CONFIG_HOME'])
                        self.assertNotIn('GOTELEMETRY',env); self.assertNotIn('GO_TELEMETRY_CHILD',env)
                    self.assertNotEqual(*identities)
                finally:
                    for lane in reversed(lanes):
                        for cap in reversed(list(lane.values())): cap.close()
    def test_missing_replaced_symlink_mode_content_and_hardlink_refuse(self):
        for change in ('missing','replaced','symlink','mode','content','hardlink'):
            with tempfile.TemporaryDirectory() as temporary,self.subTest(change=change):
                rootpath=pathlib.Path(temporary).resolve()
                with build.Directory.open_absolute(str(rootpath)) as root:
                    with root.directory('home',create=True) as home:
                        expected=build.initialize_lane_telemetry(home,build.Budget())
                        path=rootpath/'home/go/telemetry/mode'
                        if change=='missing': path.unlink()
                        elif change=='replaced':
                            path.rename(path.with_name('original'))
                            path.write_bytes(b'off\n'); path.chmod(0o600)
                        elif change=='symlink':
                            path.rename(path.with_name('original')); path.symlink_to('original')
                        elif change=='mode': path.chmod(0o644)
                        elif change=='content': path.write_bytes(b'on\n')
                        else: os.link(path,path.with_name('alias'))
                        error=FileNotFoundError if change=='missing' else OSError if change=='symlink' else build.Failure
                        with self.assertRaises(error) as refusal:
                            build.verify_lane_telemetry(home,expected,build.Budget())
                        if isinstance(refusal.exception,build.Failure):
                            wanted={'replaced':'TelemetryModeChanged','mode':'UnsafePrivateFile',
                                    'content':'TelemetryModeChanged','hardlink':'UnsafeRegularFile'}[change]
                            self.assertEqual(wanted,refusal.exception.code)
    def test_initialization_never_reuses_or_overwrites_existing_home(self):
        with tempfile.TemporaryDirectory() as temporary:
            with build.Directory.open_absolute(str(pathlib.Path(temporary).resolve())) as root:
                with root.directory('home',create=True) as home:
                    original=build.initialize_lane_telemetry(home,build.Budget())
                    with self.assertRaisesRegex(build.Failure,'^TelemetryHomeNotFresh$'):
                        build.initialize_lane_telemetry(home,build.Budget())
                    build.verify_lane_telemetry(home,original,build.Budget())
    def test_failed_readback_closes_original_created_mode_fd(self):
        with tempfile.TemporaryDirectory() as temporary:
            with build.Directory.open_absolute(str(pathlib.Path(temporary).resolve())) as root:
                with root.directory('home',create=True) as home:
                    created=[]; original=build.Directory.file
                    def capture(cap,relative,create=False,executable=False):
                        fd=original(cap,relative,create,executable)
                        if relative=='mode' and create: created.append(fd)
                        return fd
                    with mock.patch.object(build.Directory,'file',capture), \
                         mock.patch.object(build,'read_fd',return_value=b'on\n'):
                        with self.assertRaisesRegex(build.Failure,'^TelemetryModeChanged$'):
                            build.initialize_lane_telemetry(home,build.Budget())
                    self.assertEqual(1,len(created))
                    with self.assertRaises(OSError): os.fstat(created[0])
    def test_nested_trusted_root_owner_only_preserves_original_private_mode_file(self):
        # Model only the observed guest metadata mapping, not real kernel/DAC.
        # Fix the apparent process to501 so root-running unit environments also
        # exercise HOME-root refusal rather than confusing root with ownUID.
        for change in ('nested_root','foreign_go','foreign_telemetry','root_home',
                       'go_755','telemetry_755','root_mode_file'):
            with tempfile.TemporaryDirectory() as temporary,self.subTest(change=change):
                rootpath=pathlib.Path(temporary).resolve()
                with build.Directory.open_absolute(str(rootpath)) as root:
                    with root.directory('home',create=True) as home:
                        expected=list(build.initialize_lane_telemetry(home,build.Budget()))
                        expected[4]=501
                        paths={'home':rootpath/'home','go':rootpath/'home/go',
                               'telemetry':rootpath/'home/go/telemetry',
                               'mode':rootpath/'home/go/telemetry/mode'}
                        byid={(p.stat().st_dev,p.stat().st_ino):name for name,p in paths.items()}
                        original_fstat=build.os.fstat; original_stat=build.os.stat
                        def observed(st):
                            name=byid.get((st.st_dev,st.st_ino))
                            if name is None:return st
                            values={k:getattr(st,k) for k in dir(st) if k.startswith('st_')}
                            values['st_uid']=501
                            if change=='nested_root' and name in ('go','telemetry'):values['st_uid']=0
                            if change=='foreign_go' and name=='go':values['st_uid']=502
                            if change=='foreign_telemetry' and name=='telemetry':values['st_uid']=502
                            if change=='root_home' and name=='home':values['st_uid']=0
                            if change=='root_mode_file' and name=='mode':values['st_uid']=0
                            if (change=='go_755' and name=='go') or (change=='telemetry_755' and name=='telemetry'):
                                values['st_mode']=stat.S_IFMT(st.st_mode)|0o755
                            return types.SimpleNamespace(**values)
                        with mock.patch.object(build.os,'getuid',return_value=501), \
                             mock.patch.object(build.os,'fstat',side_effect=lambda fd:observed(original_fstat(fd))), \
                             mock.patch.object(build.os,'stat',side_effect=lambda *a,**kw:observed(original_stat(*a,**kw))):
                            if change=='nested_root':
                                build.verify_lane_telemetry(home,tuple(expected),build.Budget())
                            else:
                                wanted='UnsafeDirectoryOwnerOrMode' if change in ('foreign_go','foreign_telemetry') else 'UnsafePrivateFile' if change=='root_mode_file' else 'UnsafePrivateDirectory'
                                with self.assertRaises(build.Failure) as refusal:
                                    build.verify_lane_telemetry(home,tuple(expected),build.Budget())
                                self.assertEqual(wanted,refusal.exception.code)

    def test_real_lane_path_initializes_before_first_go_metadata_call(self):
        class FirstGoObserved(Exception): pass
        with tempfile.TemporaryDirectory() as temporary:
            with build.Directory.open_absolute(str(pathlib.Path(temporary).resolve())) as root:
                pair=build.BuildPair.__new__(build.BuildPair)
                pair.receipt={'lanes':[build.not_started_lane(0),build.not_started_lane(1)]}
                pair.lanes=[]; pair.telemetry_modes={}; pair.source_manifests=[]
                pair.request=sample('request'); pair.native=None; pair.supervisor=None
                pair.store=mock.Mock(root=root); pair.go=mock.Mock(path='/never-executed/go')
                pair.goroot_cap=mock.Mock(label='/goroot'); pair.seed=mock.Mock()
                pair.source_plan=mock.Mock(epoch=123)
                pair.source_plan.materialize.return_value={'files':[{'path':'go.mod','mode':'100644',
                                      'size':1,'sha256':sha(b'x')}]}
                pair._before_after=mock.Mock(return_value=('a'*64,'b'*64,'c'*64))
                def first(supervisor,go,argv,cwd,env,budget):
                    self.assertEqual(['version'],argv)
                    home=pair.lanes[0]['home']
                    build.verify_lane_telemetry(home,pair.telemetry_modes[home.identity],budget)
                    self.assertEqual(home.label,env['HOME']); self.assertEqual(home.label,env['XDG_CONFIG_HOME'])
                    raise FirstGoObserved()
                try:
                    with mock.patch.object(build,'_command_bytes',side_effect=first):
                        with self.assertRaises(FirstGoObserved): pair._lane_body(0,build.Budget())
                    self.assertEqual(1,pair._before_after.call_count)
                finally:
                    for lane in pair.lanes:
                        for cap in reversed(list(lane.values())): cap.close()


class GoToolchainPresenceControls(unittest.TestCase):
    """Original lane guard only; no Go/Git/compiler/child/native execution."""
    def test_original_lane_accepts_source_builtin_buildid_but_requires_four_binary_tools(self):
        class NativeInspectReached(Exception): pass
        required=('compile','link','asm','cgo')
        for missing in (None,)+required:
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as temporary:
                with build.Directory.open_absolute(str(pathlib.Path(temporary).resolve())) as root:
                    pair=build.BuildPair.__new__(build.BuildPair)
                    pair.receipt={'lanes':[build.not_started_lane(0),build.not_started_lane(1)]}
                    pair.lanes=[]; pair.telemetry_modes={}; pair.source_manifests=[]
                    pair.request=sample('request')
                    pair.request.update(flavour='native-linux-arm64',native={
                        name:{'path':'/not-executed/'+name} for name in ('cc','cxx','pkg_config')})
                    pair.store=mock.Mock(root=root); pair.go=mock.Mock(path='/not-executed/go')
                    pair.supervisor=object(); pair.seed=mock.Mock()
                    pair.goroot_cap=mock.Mock(label='/goroot')
                    # This is the official pinned Go1.27.3 layout: buildid is
                    # a source-built builtin, absent from preinstalled tools.
                    pair.goroot=types.SimpleNamespace(manifest={'files':[
                        {'path':'pkg/tool/linux_arm64/'+name} for name in required if name!=missing]})
                    pair.source_plan=mock.Mock(epoch=123)
                    pair.source_plan.materialize.return_value={'files':[{'path':'go.mod','mode':'100644',
                                            'size':1,'sha256':sha(b'x')}]}
                    pair._before_after=mock.Mock(return_value=('a'*64,'b'*64,'c'*64))
                    tools={name:types.SimpleNamespace(path='/not-executed/'+name) for name in ('cc','cxx','pkg_config')}
                    def inspect(supervisor,cwd,env):
                        self.assertIs(pair.supervisor,supervisor)
                        self.assertEqual(pair.lanes[0]['source'].identity,cwd.identity)
                        self.assertEqual('1',env['CGO_ENABLED'])
                        raise NativeInspectReached()
                    pair.native=types.SimpleNamespace(tools=tools,inspect=mock.Mock(side_effect=inspect))
                    def first_go(supervisor,go,argv,cwd,env,budget):
                        self.assertEqual(['version'],argv)
                        self.assertIs(pair.go,go)
                        home=pair.lanes[0]['home']
                        build.verify_lane_telemetry(home,pair.telemetry_modes[home.identity],budget)
                        return b'go version go1.27.3 linux/arm64\n',None
                    try:
                        with mock.patch.object(build,'_command_bytes',side_effect=first_go) as command:
                            if missing is None:
                                with self.assertRaises(NativeInspectReached): pair._lane_body(0,build.Budget())
                                self.assertEqual(1,pair.native.inspect.call_count)
                            else:
                                with self.assertRaises(build.Failure) as refusal: pair._lane_body(0,build.Budget())
                                self.assertEqual('GoToolchainIncomplete',refusal.exception.code)
                                self.assertEqual(0,pair.native.inspect.call_count)
                            self.assertEqual(1,command.call_count)
                            self.assertEqual(1,pair._before_after.call_count)
                    finally:
                        for lane in reversed(pair.lanes):
                            for cap in reversed(list(lane.values())): cap.close()


class StandardNativeGateControls(unittest.TestCase):
    """Mock fixed go-list bytes through the REAL closure loop; no Go/tool runs."""
    def closure(self, root, standard_fields, native_enabled=True, pkg_flags=b'-I/usr/scoped/include -lm\n'):
        with build.Directory.open_absolute(str(root)) as cap:
            lane = build.new_lane(cap, 'closure', [])
            try:
                path = 'cmd/corsa-node/main.go'; raw = b'package main\nfunc main() {}\n'
                fd = lane['source'].file(path, create=True)
                try: build._write_all(fd, raw, build.Budget())
                finally: os.close(fd)
                source = {'files': [{'path': path, 'size': len(raw), 'sha256': sha(raw), 'mode': '100644'}]}
                graph = {'Path': 'example.org/main', 'Main': True, 'Dir': lane['source'].label}
                main = {'ImportPath': 'example.org/main/cmd/corsa-node', 'Dir': lane['source'].label+'/cmd/corsa-node',
                        'Module': {'Path': 'example.org/main', 'Main': True}, 'GoFiles': ['main.go']}
                standard = dict({'ImportPath': 'runtime/cgo', 'Dir': '/goroot/src/runtime/cgo', 'Standard': True}, **standard_fields)
                request = sample('request')
                if native_enabled:
                    request.update(flavour='native-linux-arm64', native=sample('native_request'))
                    request['native']['roots'] = ['/usr/scoped']
                class Label: path = '/not-executed/pkg-config'
                class Native: tools = {'pkg_config': Label()}
                class Seed: modules = {}
                calls = []
                def fixed_bytes(supervisor, executable, argv, cwd, environment, budget, output_class='ordinary'):
                    calls.append(tuple(argv))
                    if argv == ['list', '-mod=readonly', '-m', '-json', 'all']: return build.canonical(graph), None
                    if argv[:4] == ['list', '-mod=readonly', '-deps', '-json']:
                        return build.canonical(main)+b'\n'+build.canonical(standard), None
                    if argv == ['mod', 'verify']: return b'all modules verified\n', None
                    if argv == ['--cflags', '--libs', '--', 'standard_fixture']: return pkg_flags, None
                    self.fail('unexpected tool command '+repr(argv))
                with mock.patch.object(build, '_command_bytes', side_effect=fixed_bytes):
                    try:
                        result = build.graph_and_closure(None, Label(), lane, {'GOROOT': '/goroot'}, request,
                                  Seed(), source, build.Budget(), Native() if native_enabled else None)
                    finally:
                        self.observed_commands = list(calls)
                self.assertFalse(any(x[0] == 'build' for x in calls))
                return result
            finally:
                for item in reversed(list(lane.values())): item.close()

    def test_standard_all_cgo_flag_fields_refuse_undeclared_include_before_build(self):
        for field in ('CgoCFLAGS', 'CgoCPPFLAGS', 'CgoCXXFLAGS', 'CgoFFLAGS', 'CgoLDFLAGS'):
            with tempfile.TemporaryDirectory() as temporary, self.subTest(field=field):
                with self.assertRaisesRegex(build.Failure, 'NativeFlagsUnsupported'):
                    self.closure(pathlib.Path(temporary).resolve(), {field: ['-include', '/undeclared/header']})
                self.assertFalse(any(x[0] == 'build' for x in self.observed_commands))

    def test_standard_pkg_config_is_inspected_and_dangerous_returned_paths_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(build.Failure, 'NativePathEscape'):
                self.closure(pathlib.Path(temporary).resolve(), {'CgoPkgConfig': ['standard_fixture']},
                             pkg_flags=b'-I/undeclared/include -L/undeclared/lib\n')
            self.assertIn(('--cflags', '--libs', '--', 'standard_fixture'), self.observed_commands)
            self.assertNotIn(('mod', 'verify'), self.observed_commands)

    def test_standard_supported_flags_real_goroot_scope_and_pkg_config_positive(self):
        with tempfile.TemporaryDirectory() as temporary:
            graph, closure, used = self.closure(pathlib.Path(temporary).resolve(),
                {'CgoCFLAGS': ['-I../../include', '-I/goroot/pkg/include', '-DGOOS_linux', '-pthread', '-O2', '-g'],
                 'CgoPkgConfig': ['standard_fixture']})
            self.assertEqual(set(), used)
            self.assertIn('runtime/cgo', closure['packages'])
            self.assertIn(('--cflags', '--libs', '--', 'standard_fixture'), self.observed_commands)
            self.assertEqual(('mod', 'verify'), self.observed_commands[-1])

    def test_standard_go1273_exact_cgo_flags_are_validated_in_original_closure(self):
        # Exact CFLAGS from pinned Go1.27.3 runtime/cgo/cgo.go; net also uses
        # -fno-stack-protector. No generic -f*/-W* acceptance is introduced.
        with tempfile.TemporaryDirectory() as temporary:
            graph, closure, used = self.closure(pathlib.Path(temporary).resolve(),
                {'CgoCFLAGS':['-Wall','-Werror','-fno-stack-protector','-Wdeclaration-after-statement']})
            self.assertEqual(set(),used)
            self.assertIn('runtime/cgo',closure['packages'])
            self.assertEqual(('mod','verify'),self.observed_commands[-1])

    def test_go1273_cgo_flag_lookalikes_and_native_path_guards_stay_closed(self):
        lookalikes=('-fno-stack-protector=all','-fno-stack-protector-extra',
                    '-fno-stack-protector=/undeclared/header',
                    '-Wdeclaration-after-statement=error','-Wdeclaration-after-statement-extra',
                    '-Wdeclaration-after-statement,/undeclared/header')
        for flag in lookalikes:
            with self.subTest(flag=flag),tempfile.TemporaryDirectory() as temporary:
                with self.assertRaisesRegex(build.Failure,'^NativeFlagsUnsupported$'):
                    self.closure(pathlib.Path(temporary).resolve(),
                        {'CgoCFLAGS':['-fno-stack-protector','-Wdeclaration-after-statement',flag]})
                self.assertNotIn(('mod','verify'),self.observed_commands)
        for flags in (['-I','/undeclared/headers'],['-L/undeclared/lib']):
            with self.subTest(path_flags=flags),tempfile.TemporaryDirectory() as temporary:
                with self.assertRaisesRegex(build.Failure,'^NativePathEscape$'):
                    self.closure(pathlib.Path(temporary).resolve(),
                        {'CgoCFLAGS':['-fno-stack-protector','-Wdeclaration-after-statement']+flags})
                self.assertNotIn(('mod','verify'),self.observed_commands)

    def test_standard_flags_without_native_support_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(build.Failure, 'NativeCompilerMissing'):
                self.closure(pathlib.Path(temporary).resolve(), {'CgoCFLAGS': ['-DGOOS_linux']}, native_enabled=False)


class PartialControlOwnershipControls(unittest.TestCase):
    """Actual local pipe FDs, injected failures, forbidden fork; no kernel-child claim."""
    def test_all_prefork_failure_boundaries_close_acquired_fds_and_preserve_cause(self):
        for boundary in ('first_pipe', 'second_pipe', 'nonce', 'fork'):
            with tempfile.TemporaryDirectory() as temporary, self.subTest(boundary=boundary):
                root = pathlib.Path(temporary).resolve(); budget = build.Budget()
                store = build.OutputStore(str(root/'out'), budget)
                supervisor = build.LinuxSupervisor.__new__(build.LinuxSupervisor)
                supervisor.store, supervisor.budget = store, budget
                supervisor.sequence, supervisor.poisoned, supervisor.anchor = 0, False, None
                supervisor.records, supervisor.cleanup_end = [], None
                class Exe:
                    path = '/not-executed/tool'
                    def recheck(self, *_): pass
                cause = OSError(24, 'fixture '+boundary); acquired = []; pipe_calls = [0]
                def pipe(flags):
                    self.assertEqual(os.O_NONBLOCK | os.O_CLOEXEC, flags)
                    pipe_calls[0] += 1
                    if (boundary == 'first_pipe' and pipe_calls[0] == 1) or (boundary == 'second_pipe' and pipe_calls[0] == 2): raise cause
                    ends = os.pipe()
                    for descriptor in ends:
                        os.set_blocking(descriptor, False); os.set_inheritable(descriptor, False)
                    acquired.extend(ends)
                    return ends
                def nonce(_):
                    if boundary == 'nonce': raise cause
                    return b'x'*16
                with build.Directory.open_absolute(str(root)) as cwd:
                    try:
                        with mock.patch.object(build.os, 'pipe2', side_effect=pipe, create=True), \
                             mock.patch.object(build.os, 'urandom', side_effect=nonce), \
                             mock.patch.object(build.os, 'fork', side_effect=cause) as fork:
                            with self.assertRaises(OSError) as observed:
                                supervisor.run_owned((Exe.path,), Exe(), cwd, {}, 1)
                            self.assertIs(cause, observed.exception)
                            if boundary == 'fork': fork.assert_called_once_with()
                            else: fork.assert_not_called()
                        for descriptor in acquired:
                            with self.assertRaises(OSError) as closed: os.fstat(descriptor)
                            self.assertEqual(9, closed.exception.errno)
                        self.assertEqual({}, store.open_files)
                        self.assertEqual({}, store.records)
                        self.assertEqual([], supervisor.records)
                        self.assertIsNone(supervisor.anchor)
                        self.assertFalse(supervisor.poisoned)
                        self.assertFalse((root/'out/receipt.json').exists())
                    finally:
                        # Only release OUR real pipe FDs if the pre-fix regression
                        # leaked them, so repeated fixture cases remain isolated.
                        for descriptor in acquired:
                            try: os.close(descriptor)
                            except OSError: pass
                        store.close()


class StubFixtureDescriptorControls(unittest.TestCase):
    """Bind the actual test-local replacement at the real call boundary, no fork."""
    def test_wrong_group_fixture_preserves_static_descriptor_and_twelve_argument_call(self):
        import inspect
        import types
        source = ast.parse(SCRIPT.read_text())
        owner = next(node for node in source.body if isinstance(node, ast.ClassDef) and node.name == 'LinuxSupervisor')
        run = next(node for node in owner.body if isinstance(node, ast.FunctionDef) and node.name == 'run_owned')
        calls = [node for node in ast.walk(run) if isinstance(node, ast.Call) and
                 isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and
                 node.func.value.id == 'self' and node.func.attr == '_stub']
        self.assertEqual(1, len(calls)); self.assertEqual([], calls[0].keywords)
        inventory = ['executable', 'tuple(argv)', 'environment', 'cwd.fd', 'out_fd', 'err_fd',
                     'rw', 'gr', 'rr', 'gw', 'nonce', 'ready.end']
        self.assertEqual(inventory, [ast.unparse(arg) for arg in calls[0].args])
        values = dict(executable=object(), argv=('fixture-not-executed',), environment={},
                      cwd=types.SimpleNamespace(fd=4), out_fd=1, err_fd=2,
                      rw=10, gr=11, rr=12, gw=13, nonce=b'fixture', ready=types.SimpleNamespace(end=1.0))
        arguments = [eval(compile(ast.Expression(body=copy.deepcopy(arg)), '<actual-stub-argument>', 'eval'),
                          {'tuple': tuple}, values) for arg in calls[0].args]
        self.assertEqual(12, len(arguments))
        original = inspect.getattr_static(build.LinuxSupervisor, '_stub')
        self.assertIsInstance(original, staticmethod)
        expected_parameters = ['executable', 'argv', 'environment', 'cwd_fd', 'stdout_fd', 'stderr_fd',
                               'ready_write', 'go_read', 'ready_read', 'go_write', 'nonce', 'deadline']
        self.assertEqual(expected_parameters, list(inspect.signature(original.__func__).parameters))
        inspect.signature(original.__func__).bind(*arguments)
        fixture_source = ast.parse(pathlib.Path(__file__).read_text())
        controls = next(node for node in fixture_source.body if isinstance(node, ast.ClassDef) and node.name == 'LinuxSupervisorControls')
        method = next(node for node in controls.body if isinstance(node, ast.FunctionDef) and
                      node.name == 'test_valid_ready_with_wrong_kernel_group_refuses_go_and_never_guesses_group')
        closure = next(node for node in method.body if isinstance(node, ast.FunctionDef) and node.name == 'wrong_group_stub')
        module = ast.Module(body=[copy.deepcopy(closure)], type_ignores=[])
        namespace = {}
        exec(compile(ast.fix_missing_locations(module), '<actual-wrong-group-definition>', 'exec'), namespace)
        # Defining the real closure does not execute its body or require its captured I/O values.
        self.assertEqual(expected_parameters, list(inspect.signature(namespace['wrong_group_stub']).parameters))
        replacements = [node for node in ast.walk(method) if isinstance(node, ast.Call) and
                        ast.unparse(node.func) == 'mock.patch.object' and len(node.args) == 3 and
                        ast.unparse(node.args[0]) == 'build.LinuxSupervisor' and
                        isinstance(node.args[1], ast.Constant) and node.args[1].value == '_stub']
        self.assertEqual(1, len(replacements)); self.assertEqual([], replacements[0].keywords)
        patch = eval(compile(ast.Expression(body=copy.deepcopy(replacements[0])), '<actual-stub-patch>', 'eval'),
                     {'mock': mock, 'build': build, 'wrong_group_stub': namespace['wrong_group_stub']})
        instance = object.__new__(build.LinuxSupervisor)
        with mock.patch.object(build.os, 'fork', side_effect=AssertionError('pure binding control must not fork')) as fork, patch:
            bound = inspect.signature(instance._stub).bind(*arguments)
            self.assertEqual(expected_parameters, list(bound.arguments))
            self.assertIs(arguments[0], bound.arguments['executable'])
            for name, value in zip(expected_parameters, arguments): self.assertIs(value, bound.arguments[name])
            fork.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
