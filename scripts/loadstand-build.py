#!/usr/bin/env python3
"""T5g v3/schema2: Linux-owned, offline two-lane build provenance.

No built artifact is executed. This is not a sandbox, resource admission or
runtime acceptance. Linux lifecycle validation requires its own qualified run.
"""
from __future__ import annotations

import argparse
import base64
import bisect
import configparser
import ctypes
import dataclasses
import errno
import hashlib
import io
import json
import os
import platform
import re
import select
import shlex
import signal
import stat
import struct
import zlib
import datetime
import sys
import time
import unicodedata
import zipfile
from pathlib import Path

MIB = 1024 ** 2
GIB = 1024 ** 3
LIMITS = {
    'request_json': MIB, 'overlay_json': 16*MIB, 'seed_json': 16*MIB,
    'receipt_json': 16*MIB, 'manifest_json': 32*MIB, 'depth': 16,
    'values': 300000, 'array': 100000, 'string': 65536, 'path': 1024,
    'path_depth': 32, 'argv': 4096, 'targets': 3, 'source_files': 100000,
    'source_file': 64*MIB, 'source_total': 2*GIB, 'modules': 4096,
    'seed_objects': 20480, 'mod': MIB, 'info': 65536, 'ziphash': 128,
    'zip': 256*MIB, 'seed_total': 8*GIB, 'zip_entries': 100000,
    'all_zip_entries': 500000, 'module_expand': 2*GIB, 'lane_expand': 16*GIB,
    'tool_entries': 100000, 'tool_file': 512*MIB, 'tool_total': 8*GIB,
    'closure_files': 250000, 'packages': 50000, 'stdout': 8*MIB,
    'stderr': 8*MIB, 'object_stdout': 64*MIB, 'graph_stdout': 64*MIB,
    'evidence': 512*MIB, 'artifact': 512*MIB, 'lane_artifacts': 1536*MIB,
    'gocache': 8*GIB, 'working': 64*GIB, 'children': 4096,
    'child_bytes': 128*1024, 'control': 256,
    'ready_seconds': 5, 'command_seconds': 30, 'graph_seconds': 300,
    'build_seconds': 1800, 'lane_seconds': 2700, 'overall_seconds': 6000,
    'monitor_ms': 100, 'term_seconds': 2, 'kill_seconds': 3,
    'shutdown_seconds': 10, 'delete_seconds': 10,
}
TARGETS = {'corsa-node': './cmd/corsa-node',
           'sdkedge': './internal/testutil/loadstand/sdkedge',
           'linuxhelper': './internal/testutil/loadstand/linuxhelper',
           'fullnode': './internal/testutil/loadstand/fullnode'}
GO_VERSION = 'go1.27.3'
FLAVOURS = ('native-linux-arm64', 'cross-compile-cgo0')
STATES = ('not_started', 'present_verified', 'unavailable', 'invalid', 'unconfirmed')
HASH = r'^(?!0{64}$)[0-9a-f]{64}$'
OID = r'^(?!0{40}$)[0-9a-f]{40}$'
H1 = r'^h1:[A-Za-z0-9+/]{43}=$'
MODULE_VERSION = r'^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+incompatible)?$'
DOMAIN_NAMES = frozenset(('source', 'seed', 'closure', 'request.canonical', 'outputs',
                          'toolchain', 'native', 'graph', 'command', 'lane'))


class Failure(ValueError):
    def __init__(self, code, phase='validation', incomplete=False):
        super().__init__(code)
        self.code, self.phase, self.incomplete = code, phase, incomplete


def fail(code, phase='validation', incomplete=False):
    raise Failure(code, phase, incomplete)


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail('DuplicateKey')
        result[key] = value
    return result


def _reject_number(_):
    fail('NonIntegerNumber')


def _lexical_depth(raw):
    depth, quoted, escaped = 0, False, False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
            continue
        if byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            if depth > LIMITS['depth']:
                fail('ValidationLimit')
        elif byte in (93, 125):
            depth -= 1
            if depth < 0:
                fail('InvalidJSON')


def _json_limits(value, depth=0, budget=None):
    if budget is None:
        budget = [0]
    budget[0] += 1
    if depth > LIMITS['depth'] or budget[0] > LIMITS['values']:
        fail('ValidationLimit')
    if type(value) is str:
        try:
            encoded = value.encode('utf-8')
        except UnicodeError:
            fail('InvalidUTF8')
        if len(encoded) > LIMITS['string']:
            fail('ValidationLimit')
    elif type(value) is list:
        if len(value) > LIMITS['array']:
            fail('ValidationLimit')
        for item in value:
            _json_limits(item, depth+1, budget)
    elif type(value) is dict:
        for key, item in value.items():
            _json_limits(key, depth+1, budget)
            _json_limits(item, depth+1, budget)
    elif value is not None and type(value) not in (bool, int):
        fail('InvalidJSONType')


def decode_json(raw, limit=LIMITS['request_json']):
    if type(raw) is not bytes or len(raw) > limit:
        fail('InputTooLarge')
    _lexical_depth(raw)
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_pairs,
                           parse_float=_reject_number, parse_constant=_reject_number)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise Failure('InvalidJSON') from exc
    _json_limits(value)
    return value


def canonical(value):
    _json_limits(value)
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def domain_hash(name, value):
    if name not in DOMAIN_NAMES:
        fail('UnknownHashDomain')
    if name == 'source':
        return source_digest(value)
    return hashlib.sha256(('corsa.t5g.v2.'+name).encode('ascii')+b'\0'+canonical(value)).hexdigest()


def validate_version(value):
    if type(value) is not int or value != 2:
        fail('SchemaVersion')
    return value


def relative_path(value):
    if type(value) is not str or not value or len(value.encode()) > LIMITS['path']:
        fail('UnsafePath')
    parts = value.split('/')
    if len(parts) > LIMITS['path_depth'] or any(p in ('', '.', '..') for p in parts):
        fail('UnsafePath')
    if '\\' in value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        fail('UnsafePath')
    return value


def absolute_path(value):
    if type(value) is not str or not value.startswith('/'):
        fail('AbsolutePathRequired')
    relative_path(value[1:])
    return value


# Compact closed runtime definitions. JSON Schema is generated from this table;
# an independent test interpreter checks the supported schema subset.
def S(pattern=None):
    return {'kind': 'str', 'pattern': pattern}


def I(maximum, minimum=0):
    return {'kind': 'int', 'min': minimum, 'max': maximum}


def E(*values):
    return {'kind': 'enum', 'values': list(values)}


def R(name):
    return {'kind': 'ref', 'name': name}


def A(item, maximum, minimum=0, unique=False):
    return {'kind': 'array', 'item': item, 'min': minimum, 'max': maximum, 'unique': unique}


def N(item):
    return {'kind': 'nullable', 'item': item}


B = {'kind': 'bool'}
P = {'kind': 'path'}
ABS = {'kind': 'absolute'}
D = S(HASH)
O = S(OID)
DEFS = {
    'tool': {'path': ABS, 'sha256': D},
    'blob': {'oid': O, 'sha256': D, 'size': I(LIMITS['source_file']), 'mode': E('100644', '100755')},
    'origin': {'kind': E('versioned', 'staged_uncommitted'), 'commit': N(O),
               'tree': N(O), 'path': P, 'blob': N(O),
               'staged_capture_manifest_sha256': N(D),
               'role': N(E('candidate_change', 'instrumentation', 'harness'))},
    'overlay': {'op': E('add', 'modify', 'delete'), 'path': P,
                'before': N(R('blob')), 'after': N(R('blob')), 'origin': R('origin')},
    'git': {'directory': ABS, 'executable': R('tool'), 'base_commit': O, 'base_tree': O},
    'seed_request': {'root': ABS, 'manifest_path': ABS, 'manifest_sha256': D},
    'toolchain_request': {'go': R('tool'), 'goroot': ABS, 'manifest_path': ABS, 'manifest_sha256': D},
    'native_request': {'roots': A(ABS, 32, 1, True), 'cc': R('tool'), 'cxx': R('tool'), 'pkg_config': R('tool'),
                       'ld': R('tool'), 'ar': R('tool'), 'manifest_path': ABS, 'manifest_sha256': D},
    'image': {'state': E('absent_native_host', 'externally_declared_unverified'), 'digest': N(D)},
    'request': {'schema_version': E(2), 'git': R('git'),
                'overlay': A(R('overlay'), LIMITS['source_files']),
                'seed': R('seed_request'), 'toolchain': R('toolchain_request'),
                'native': N(R('native_request')), 'flavour': E(*FLAVOURS),
                'targets': A(E(*TARGETS), 3, 1, True), 'keep_work': B, 'image': R('image')},
    'file': {'path': P, 'size': I(LIMITS['tool_file']), 'sha256': D, 'mode': E('100644', '100755')},
    'source_file': {'path': P, 'size': I(LIMITS['source_file']), 'sha256': D,
                    'mode': E('100644', '100755'), 'blob': O, 'origin': R('origin')},
    'source': {'schema_version': E(2), 'base_commit': O, 'base_tree': O,
               'epoch': I(2**63-1), 'files': A(R('source_file'), LIMITS['source_files'], 1)},
    'fingerprint': {'schema_version': E(2), 'files': A(R('file'), LIMITS['tool_entries'], 1)},
    'seed_file': {'path': P, 'size': I(LIMITS['zip']), 'sha256': D,
                  'kind': E('mod', 'info', 'zip', 'ziphash')},
    'seed_module': {'path': S(), 'version': S(MODULE_VERSION),
                    'files': A(R('seed_file'), 4, 1)},
    'seed': {'schema_version': E(2), 'modules': A(R('seed_module'), LIMITS['modules'])},
    'evidence': {'state': E(*STATES), 'sha256': N(D), 'reason': N(S())},
    'failure': {'code': S(), 'phase': S()},
    'cleanup': {'state': E('complete', 'kept_work', 'unconfirmed', 'incomplete'), 'possible_writers': B},
    'artifact': {'lane': I(1), 'target': E(*TARGETS), 'size': I(LIMITS['artifact'], 1), 'sha256': D,
                  'reported_version_state': E('deferred_to_owned_runtime'),
                  'runtime_exe_state': E('deferred_to_owned_runtime')},
    'lane': {'index': I(1), 'state': E('not_started', 'complete', 'failed', 'incomplete'),
              'source_before': N(D), 'source_after': N(D), 'seed_before': N(D),
              'seed_after': N(D), 'toolchain_before': N(D), 'toolchain_after': N(D),
              'closure_sha256': N(D), 'cache_initially_empty': N(B), 'joined': N(B),
              'targets': A(E(*TARGETS), 3, 0, True)},
    'host': {'system': S(), 'architecture': S()},
    'environment': {'go_version': E(GO_VERSION), 'goos': E('linux'), 'goarch': E('arm64'),
                     'goarm64': E('v8.0'), 'cgo': E(0, 1), 'goexperiment': E(''),
                     'vcs': E('disabled_archive'), 'network_isolation': E('not_established'),
                     'image': R('image')},
    'supervisor': {'state': E(*STATES), 'all_joined': N(B), 'commands': I(100000),
                    'reason': N(S())},
    'receipt': {'schema_version': E(2), 'request_raw_sha256': D, 'request_canonical_sha256': D,
                 'status': E('verified_equal', 'failed', 'incomplete'), 'source': R('evidence'),
                 'origins': A(R('origin'), LIMITS['source_files']), 'flavour': E(*FLAVOURS),
                 'host': R('host'), 'environment': R('environment'),
                 'toolchain': R('evidence'), 'native': R('evidence'), 'offline': R('evidence'),
                 'supervisor': R('supervisor'), 'lanes': A(R('lane'), 2, 2),
                 'artifacts': A(R('artifact'), 6), 'comparison': E('not_established', 'equal', 'different'),
                 'limitations': A(S(), 100, 1, True), 'failure': N(R('failure')),
                 'cleanup': R('cleanup'), 'output_manifest_sha256': D},
    'output_file': {'path': P, 'size': I(LIMITS['evidence']), 'sha256': D, 'mode': E('100600'),
                    'role': E('request', 'source', 'seed', 'toolchain', 'native', 'graph', 'closure',
                              'command_stdout', 'command_stderr', 'command_record', 'lane_check',
                              'module_checks', 'artifact', 'metadata', 'comparison', 'failure', 'checkpoint'),
                    'completeness': E('complete', 'failed_evidence')},
    'outputs': {'schema_version': E(2), 'files': A(R('output_file'), LIMITS['closure_files'])},
}


def _validate_field(value, spec):
    kind = spec['kind']
    if kind == 'nullable':
        if value is not None:
            _validate_field(value, spec['item'])
        return
    if kind == 'ref':
        validate(spec['name'], value)
        return
    if kind == 'bool':
        if type(value) is not bool:
            fail('ExpectedBool')
        return
    if kind == 'int':
        if type(value) is not int or not spec['min'] <= value <= spec['max']:
            fail('ExpectedIntegerRange')
        return
    if kind == 'enum':
        if not any(type(value) is type(item) and value == item for item in spec['values']):
            fail('UnknownEnum')
        return
    if kind in ('str', 'path', 'absolute'):
        if type(value) is not str or not value or len(value.encode('utf-8')) > LIMITS['string']:
            fail('ExpectedString')
        if spec.get('pattern') and not re.fullmatch(spec['pattern'], value):
            fail('InvalidDigestOrSyntax')
        if kind == 'path':
            relative_path(value)
        elif kind == 'absolute':
            absolute_path(value)
        return
    if kind == 'array':
        if type(value) is not list or not spec['min'] <= len(value) <= spec['max']:
            fail('ArrayLimit')
        for item in value:
            _validate_field(item, spec['item'])
        if spec['unique'] and len({canonical(x) for x in value}) != len(value):
            fail('DuplicateArrayValue')
        return
    fail('UnknownValidatorKind')


def validate(name, value):
    _json_limits(value)
    fields = DEFS.get(name)
    if fields is None or type(value) is not dict or set(value) != set(fields):
        fail('ClosedObjectFields')
    for key, spec in fields.items():
        _validate_field(value[key], spec)
    semantic = SEMANTIC.get(name)
    if semantic:
        semantic(value)
    return value


def _origin(v):
    if v['kind'] == 'versioned':
        if v['commit'] is None or v['tree'] is None or v['staged_capture_manifest_sha256'] is not None:
            fail('OriginBinding')
    elif v['commit'] is not None or v['tree'] is not None or v['staged_capture_manifest_sha256'] is None:
        fail('OriginBinding')


def _overlay(v):
    required = {'add': (False, True), 'modify': (True, True), 'delete': (True, False)}[v['op']]
    if (v['before'] is not None, v['after'] is not None) != required:
        fail('OverlayBinding')
    if v['origin']['path'] != v['path'] or v['origin']['blob'] != (v['after']['oid'] if v['after'] else None):
        fail('OriginBinding')


def _evidence(v):
    good = v['state'] == 'present_verified'
    if good != (v['sha256'] is not None) or good == (v['reason'] is not None):
        fail('EvidenceDiscriminant')


def _request(v):
    if (v['flavour'] == 'native-linux-arm64') != (v['native'] is not None):
        fail('NativeFlavourMismatch')
    paths = [x['path'] for x in v['overlay']]
    if len(set(paths)) != len(paths):
        fail('DuplicateOverlayPath')
    if (v['image']['state'] == 'absent_native_host') != (v['image']['digest'] is None):
        fail('ImageDiscriminant')


def _lane(v):
    if v['state'] == 'not_started':
        if any(v[k] is not None for k in v if k not in ('state', 'index', 'targets')) or v['targets']:
            fail('LaneNotStarted')
    if v['state'] == 'complete':
        if any(v[k] is None for k in v if k not in ('state', 'index', 'targets')) or not v['joined'] or not v['cache_initially_empty'] or not v['targets']:
            fail('LaneIncomplete')


def _receipt(v):
    if [x['index'] for x in v['lanes']] != [0, 1]:
        fail('LaneIndex')
    if (v['flavour'] == 'native-linux-arm64') != (v['environment']['cgo'] == 1):
        fail('NativeFlavourMismatch')
    if v['cleanup']['possible_writers'] and v['status'] != 'incomplete':
        fail('UnconfirmedWriters')
    if v['status'] == 'verified_equal':
        if v['failure'] is not None or v['comparison'] != 'equal' or v['cleanup']['state'] not in ('complete', 'kept_work'):
            fail('FalseVerified')
        if not v['supervisor']['all_joined'] or v['supervisor']['state'] != 'present_verified':
            fail('FalseVerified')
        for k in ('source', 'toolchain', 'offline'):
            if v[k]['state'] != 'present_verified':
                fail('FalseVerified')
        if v['flavour'] == 'native-linux-arm64' and v['native']['state'] != 'present_verified':
            fail('FalseVerified')
        source = v['source']['sha256']
        for lane in v['lanes']:
            if lane['state'] != 'complete' or lane['source_before'] != source or lane['source_after'] != source:
                fail('SourceConjunction')
            for prefix in ('seed', 'toolchain'):
                bound = v['offline' if prefix == 'seed' else 'toolchain']['sha256']
                if lane[prefix+'_before'] != lane[prefix+'_after'] or lane[prefix+'_before'] != bound:
                    fail('InputMutation')
        for prefix in ('seed_before', 'toolchain_before', 'closure_sha256'):
            if v['lanes'][0][prefix] != v['lanes'][1][prefix]:
                fail('InputMutation')
        by_lane = [{a['target']: a for a in v['artifacts'] if a['lane'] == n} for n in (0, 1)]
        for n in (0, 1):
            if len(by_lane[n]) != len([a for a in v['artifacts'] if a['lane'] == n]) or set(by_lane[n]) != set(v['lanes'][n]['targets']):
                fail('ArtifactSet')
        if set(by_lane[0]) != set(by_lane[1]) or any((by_lane[0][k]['sha256'], by_lane[0][k]['size']) != (by_lane[1][k]['sha256'], by_lane[1][k]['size']) for k in by_lane[0]):
            fail('ArtifactMismatch')
    elif v['failure'] is None:
        fail('MissingFailure')


def _outputs(v):
    paths = [x['path'] for x in v['files']]
    if paths != sorted(set(paths)):
        fail('OutputMembership')
    if any(p in ('artifacts-manifest.json', 'receipt.json', 'receipt.sha256') or 'runtime-receipt' in p for p in paths):
        fail('ReceiptCycle')


SEMANTIC = {'origin': _origin, 'overlay': _overlay, 'evidence': _evidence,
            'request': _request, 'lane': _lane, 'receipt': _receipt, 'outputs': _outputs}


def _schema_field(spec):
    k = spec['kind']
    if k == 'ref':
        return {'$ref': '#/$defs/'+spec['name']}
    if k == 'nullable':
        return {'oneOf': [{'type': 'null'}, _schema_field(spec['item'])]}
    if k == 'bool':
        return {'type': 'boolean'}
    if k == 'int':
        return {'type': 'integer', 'minimum': spec['min'], 'maximum': spec['max']}
    if k == 'enum':
        return {'enum': spec['values']}
    if k == 'array':
        return {'type': 'array', 'items': _schema_field(spec['item']), 'minItems': spec['min'],
                'maxItems': spec['max'], 'uniqueItems': spec['unique']}
    result = {'type': 'string', 'minLength': 1, 'maxLength': LIMITS['string']}
    if spec.get('pattern'):
        result['pattern'] = spec['pattern']
    if k in ('path', 'absolute'):
        result['maxLength'] = LIMITS['path']
        result['pattern'] = (r'^/' if k == 'absolute' else r'^(?!/)')+r'(?!\.{1,2}(?:/|$))(?!.*(?:/\.{1,2})(?:/|$))(?!.*//)(?!.*[\\\x00-\x1f\x7f]).+(?<!/)$'
    return result


def schema_document():
    defs = {name: {'type': 'object', 'additionalProperties': False, 'required': sorted(fields),
                   'properties': {key: _schema_field(spec) for key, spec in fields.items()}}
            for name, fields in DEFS.items()}
    # Structural discriminants are expressed in the schema too. Dynamic digest
    # equality and filesystem authorities necessarily remain runtime invariants.
    defs['origin']['allOf'] = [
        {'if': {'properties': {'kind': {'const': 'versioned'}}},
         'then': {'properties': {'commit': {'type': 'string'}, 'tree': {'type': 'string'}, 'staged_capture_manifest_sha256': {'type': 'null'}}},
         'else': {'properties': {'commit': {'type': 'null'}, 'tree': {'type': 'null'}, 'staged_capture_manifest_sha256': {'type': 'string'}}}}]
    defs['evidence']['allOf'] = [
        {'if': {'properties': {'state': {'const': 'present_verified'}}},
         'then': {'properties': {'sha256': {'type': 'string'}, 'reason': {'type': 'null'}}},
         'else': {'properties': {'sha256': {'type': 'null'}, 'reason': {'type': 'string'}}}}]
    defs['request']['allOf'] = [
        {'if': {'properties': {'flavour': {'const': 'native-linux-arm64'}}},
         'then': {'properties': {'native': {'type': 'object'}}},
         'else': {'properties': {'native': {'type': 'null'}}}}]
    defs['image']['allOf'] = [{'if': {'properties': {'state': {'const': 'absent_native_host'}}}, 'then': {'properties': {'digest': {'type': 'null'}}}, 'else': {'properties': {'digest': {'type': 'string'}}}}]
    defs['native_file']['allOf'] = [{'if': {'properties': {'kind': {'const': 'regular'}}}, 'then': {'properties': {'regular': {'type': 'object'}, 'link': {'type': 'null'}}}, 'else': {'properties': {'regular': {'type': 'null'}, 'link': {'type': 'object'}}}}]
    defs['receipt']['allOf'] = [
        {'if': {'properties': {'status': {'const': 'verified_equal'}}},
         'then': {'properties': {'failure': {'type': 'null'}, 'comparison': {'const': 'equal'},
                                 'cleanup': {'properties': {'possible_writers': {'const': False}, 'state': {'enum': ['complete', 'kept_work']}}}}},
         'else': {'properties': {'failure': {'type': 'object'}}}}]
    defs['overlay']['allOf'] = [
        {'if': {'properties': {'op': {'const': op}}},
         'then': {'properties': {'before': {'type': before}, 'after': {'type': after}}}}
        for op, before, after in (('add', 'null', 'object'), ('modify', 'object', 'object'), ('delete', 'object', 'null'))]
    digest_fields = ('source_before', 'source_after', 'seed_before', 'seed_after', 'toolchain_before', 'toolchain_after', 'closure_sha256')
    defs['lane']['allOf'] = [
        {'if': {'properties': {'state': {'const': 'not_started'}}},
         'then': {'properties': dict({key: {'type': 'null'} for key in digest_fields+('joined', 'cache_initially_empty')}, targets={'maxItems': 0})}},
        {'if': {'properties': {'state': {'const': 'complete'}}},
         'then': {'properties': dict({key: {'type': 'string'} for key in digest_fields}, joined={'const': True}, cache_initially_empty={'const': True}, targets={'minItems': 1})}}]
    defs['command']['allOf'] = [
        {'if': {'properties': {'joined': {'const': True}}},
         'then': {'oneOf': [{'properties': {'exit_code': {'type': 'integer'}, 'term_signal': {'type': 'null'}}},
                            {'properties': {'exit_code': {'type': 'null'}, 'term_signal': {'type': 'integer'}}}]},
         'else': {'properties': {'exit_code': {'type': 'null'}, 'term_signal': {'type': 'null'}, 'failure': {'type': 'string'}}}}]
    defs['receipt']['allOf'] += [
        {'if': {'properties': {'flavour': {'const': 'native-linux-arm64'}}},
         'then': {'properties': {'environment': {'properties': {'cgo': {'const': 1}}}}},
         'else': {'properties': {'environment': {'properties': {'cgo': {'const': 0}}}}}},
        {'if': {'properties': {'status': {'const': 'verified_equal'}}},
         'then': {'properties': dict({key: {'properties': {'state': {'const': 'present_verified'}}} for key in ('source', 'toolchain', 'offline')},
                     supervisor={'properties': {'state': {'const': 'present_verified'}, 'all_joined': {'const': True}}},
                     lanes={'items': {'properties': {'state': {'const': 'complete'}}}})}},
        {'if': {'properties': {'cleanup': {'properties': {'possible_writers': {'const': True}}}}},
         'then': {'properties': {'status': {'const': 'incomplete'}}}}]
    return {'$schema': 'https://json-schema.org/draft/2020-12/schema', '$id': 'urn:corsa:t5g:v2',
            '$comment': 'Runtime additionally checks UTF-8 byte/depth/value/path limits, dynamic source/seed/toolchain/artifact equality, exact ownership and real FD evidence. This is not a generic JSON Schema engine.',
            'oneOf': [{'$ref': '#/$defs/request'}, {'$ref': '#/$defs/receipt'},
                      {'$ref': '#/$defs/source'}, {'$ref': '#/$defs/seed'},
                      {'$ref': '#/$defs/outputs'}, {'$ref': '#/$defs/fingerprint'}, {'$ref': '#/$defs/native_manifest'}], '$defs': defs}


class Budget:
    def __init__(self, end=None, cancelled=None):
        self.end = time.monotonic()+LIMITS['overall_seconds'] if end is None else end
        self.cancelled = cancelled if cancelled is not None else [False]

    def check(self):
        if self.cancelled[0]:
            fail('Cancelled', 'lifetime', True)
        if time.monotonic() >= self.end:
            fail('OverallTimeout', 'lifetime', True)

    def child(self, seconds):
        return Budget(min(self.end, time.monotonic()+seconds), self.cancelled)


def stable_stat(st):
    return (st.st_dev, st.st_ino, st.st_size, st.st_mode, st.st_uid,
            st.st_nlink, st.st_mtime_ns, st.st_ctime_ns)


def check_regular(fd, maximum, private=False, executable=False):
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_size > maximum:
        fail('UnsafeRegularFile')
    if st.st_uid not in (0, os.getuid()) or st.st_mode & 0o6022:
        fail('UnsafeFileOwnerOrMode')
    if private and (st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o600):
        fail('UnsafePrivateFile')
    if executable and not st.st_mode & 0o111:
        fail('ExecutableRequired')
    return st


def read_fd(fd, maximum, budget, private=False):
    before = check_regular(fd, maximum, private)
    os.lseek(fd, 0, os.SEEK_SET)
    chunks, size = [], 0
    while True:
        budget.check()
        data = os.read(fd, min(MIB, maximum+1-size))
        if not data:
            break
        size += len(data)
        if size > maximum:
            fail('InputTooLarge')
        chunks.append(data)
    after = check_regular(fd, maximum, private)
    if stable_stat(before) != stable_stat(after) or size != before.st_size:
        fail('InputMutation', 'input', True)
    return b''.join(chunks)


def hash_fd(fd, maximum, budget):
    before = check_regular(fd, maximum)
    os.lseek(fd, 0, os.SEEK_SET)
    digest, size = hashlib.sha256(), 0
    while True:
        budget.check()
        chunk = os.read(fd, min(MIB, maximum+1-size))
        if not chunk:
            break
        size += len(chunk)
        if size > maximum:
            fail('InputTooLarge')
        digest.update(chunk)
    if stable_stat(before) != stable_stat(check_regular(fd, maximum)) or size != before.st_size:
        fail('InputMutation', 'input', True)
    return digest.hexdigest(), before


class Directory:
    """Pinned traversal capability; absolute path is diagnostic, never authority."""
    def __init__(self, fd, label):
        self.fd, self.label = fd, label
        st = os.fstat(fd)
        if not stat.S_ISDIR(st.st_mode):
            fail('DirectoryRequired')
        self.identity = (st.st_dev, st.st_ino)

    @classmethod
    def open_absolute(cls, path):
        if path != '/':
            absolute_path(path)
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            for component in (path[1:].split('/') if path != '/' else []):
                nxt = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                os.close(fd)
                fd = nxt
                st = os.fstat(fd)
                if st.st_uid not in (0, os.getuid()) or (st.st_mode & 0o022 and not st.st_mode & stat.S_ISVTX):
                    fail('UnsafeDirectoryOwnerOrMode')
            result = cls(fd, path)
            fd = None
            return result
        finally:
            if fd is not None:
                os.close(fd)

    def directory(self, relative, create=False):
        relative_path(relative)
        fd = os.dup(self.fd)
        try:
            for part in relative.split('/'):
                if create:
                    try:
                        os.mkdir(part, 0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                    else:
                        os.fsync(fd)
                nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                os.close(fd)
                fd = nxt
                st = os.fstat(fd)
                if st.st_uid not in (0, os.getuid()) or st.st_mode & 0o6022:
                    fail('UnsafeDirectoryOwnerOrMode')
            result = Directory(fd, self.label+'/'+relative)
            fd = None
            return result
        finally:
            if fd is not None:
                os.close(fd)

    def file(self, relative, create=False, executable=False):
        relative_path(relative)
        parts = relative.rsplit('/', 1)
        parent = self.directory(parts[0], create) if len(parts) == 2 else None
        try:
            flags = os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
            flags |= os.O_RDWR | os.O_CREAT | os.O_EXCL if create else os.O_RDONLY
            fd = os.open(parts[-1], flags, 0o600, dir_fd=parent.fd if parent else self.fd)
            try:
                check_regular(fd, LIMITS['tool_file'], private=create, executable=executable)
                return fd
            except BaseException:
                os.close(fd)
                raise
        finally:
            if parent:
                parent.close()

    def verify_name(self):
        with Directory.open_absolute(self.label) as current:
            if current.identity != self.identity:
                fail('InputMutation', 'directory', True)

    def names(self):
        return sorted(os.listdir(self.fd))

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def open_absolute_file(path, maximum, budget, executable=False):
    absolute_path(path)
    parent, name = path.rsplit('/', 1)
    with Directory.open_absolute(parent) as directory:
        fd = directory.file(name, executable=executable)
    try:
        check_regular(fd, maximum, executable=executable)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _write_all(fd, data, budget):
    offset = 0
    while offset < len(data):
        budget.check()
        written = os.write(fd, data[offset:offset+MIB])
        if written <= 0:
            fail('ShortWrite', 'output', True)
        offset += written


class OutputStore:
    def __init__(self, path, budget):
        absolute_path(path)
        parent, name = path.rsplit('/', 1)
        with Directory.open_absolute(parent) as cap:
            os.mkdir(name, 0o700, dir_fd=cap.fd)  # fresh only; no exist_ok
            os.fsync(cap.fd)
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=cap.fd)
        self.root = Directory(fd, path)
        self.budget, self.records, self.open_files = budget, {}, {}
        self.total = 0

    def create(self, relative, role):
        if relative in self.records or relative in self.open_files:
            fail('DuplicateOutput')
        fd = self.root.file(relative, create=True)
        self.open_files[relative] = (fd, role)
        return fd

    def finish(self, relative, complete=True):
        fd, role = self.open_files.pop(relative)
        try:
            digest, st = hash_fd(fd, LIMITS['artifact'] if role == 'artifact' else LIMITS['evidence'], self.budget)
            check_regular(fd, st.st_size, private=True)
            os.fsync(fd)
            parent_path = relative.rsplit('/', 1)[0] if '/' in relative else None
            if parent_path:
                with self.root.directory(parent_path) as parent:
                    os.fsync(parent.fd)
            else:
                os.fsync(self.root.fd)
            if role != 'artifact':
                self.total += st.st_size
                if self.total > LIMITS['evidence']:
                    fail('EvidenceLimit', 'output', True)
            self.records[relative] = {'path': relative, 'size': st.st_size, 'sha256': digest,
                                      'mode': '100600', 'role': role,
                                      'completeness': 'complete' if complete else 'failed_evidence'}
        finally:
            os.close(fd)
        return self.records[relative]

    def put(self, relative, data, role):
        fd = self.create(relative, role)
        _write_all(fd, data, self.budget)
        return self.finish(relative)

    def json(self, relative, value, role):
        return self.put(relative, canonical(value), role)

    def abandon(self, relative):
        item = self.open_files.pop(relative, None)
        if item:
            os.close(item[0])
        # A possible writer retains its own descriptor. Path is deliberately
        # retained but excluded from immutable content evidence.

    def verified_manifest(self):
        if self.open_files:
            fail('UnclosedEvidence', 'output', True)
        for path, expected in self.records.items():
            fd = self.root.file(path)
            try:
                digest, st = hash_fd(fd, LIMITS['artifact'] if expected['role'] == 'artifact' else LIMITS['evidence'], self.budget)
                check_regular(fd, st.st_size, private=True)
                if digest != expected['sha256'] or st.st_size != expected['size']:
                    fail('EvidenceMutation', 'output', True)
            finally:
                os.close(fd)
        value = {'schema_version': 2, 'files': [self.records[p] for p in sorted(self.records)]}
        validate('outputs', value)
        return value

    def close(self):
        for relative in list(self.open_files):
            self.abandon(relative)
        self.root.close()

    def publish_receipt(self, receipt):
        validate('receipt', receipt)
        raw = canonical(receipt)
        if len(raw) > LIMITS['receipt_json']:
            fail('IncompleteReceipt', 'output', True)
        # Terminal files deliberately never enter the artifacts manifest.
        fd = self.root.file('receipt.tmp', create=True)
        try:
            _write_all(fd, raw, self.budget)
            os.fsync(fd)
        finally:
            os.close(fd)
        fd = self.root.file('receipt.sha256', create=True)
        try:
            _write_all(fd, (hashlib.sha256(raw).hexdigest()+'\n').encode(), self.budget)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(self.root.fd)
        libc = ctypes.CDLL(None, use_errno=True)
        rename = getattr(libc, 'renameat2', None)
        if rename is None or rename(self.root.fd, b'receipt.tmp', self.root.fd, b'receipt.json', 1) != 0:
            fail('ReceiptPublishUnconfirmed', 'output', True)
        os.fsync(self.root.fd)


@dataclasses.dataclass(frozen=True)
class Executable:
    path: str
    fd: int
    sha256: str
    initial: tuple

    @classmethod
    def pin(cls, spec, budget):
        validate('tool', spec)
        # Resolve once; actual execution and rechecks use the retained regular FD.
        resolved = os.path.realpath(spec['path'])
        fd = open_absolute_file(resolved, LIMITS['tool_file'], budget, executable=True)
        try:
            digest, st = hash_fd(fd, LIMITS['tool_file'], budget)
            if digest != spec['sha256']:
                fail('ToolFingerprintMismatch')
            return cls(resolved, fd, digest, stable_stat(st))
        except BaseException:
            os.close(fd)
            raise

    def recheck(self, budget):
        digest, st = hash_fd(self.fd, LIMITS['tool_file'], budget)
        current = os.stat(self.path, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (st.st_dev, st.st_ino):
            fail('InputMutation', 'tool_path', True)
        if digest != self.sha256 or stable_stat(st) != self.initial:
            fail('InputMutation', 'tool', True)

    def close(self):
        os.close(self.fd)


@dataclasses.dataclass(frozen=True)
class CommandResult:
    sequence: int
    argv: tuple
    stdout: str
    stderr: str
    exit_code: int | None
    term_signal: int | None
    joined: bool
    failure: str | None
    signals: tuple

    def as_json(self):
        value = dataclasses.asdict(self)
        value['schema_version'] = 2
        value['argv'] = list(self.argv)
        value['signals'] = [{'signal': sig, 'outcome': outcome} for sig, outcome in self.signals]
        validate('command', value)
        return value


def proc_stat(pid):
    try:
        with open('/proc/'+str(pid)+'/stat', 'rb', buffering=0) as file:
            raw = file.read(LIMITS['child_bytes']+1)
        if len(raw) > LIMITS['child_bytes']:
            fail('SupervisorLimit', 'supervisor', True)
        end = raw.rfind(b') ')
        fields = raw[end+2:].split()
        if end < 0 or len(fields) < 20:
            fail('ProcIdentityUnknown', 'supervisor', True)
        return {'pid': pid, 'state': fields[0].decode('ascii'), 'ppid': int(fields[1]),
                'pgid': int(fields[2]), 'sid': int(fields[3]), 'start': int(fields[19])}
    except (OSError, ValueError, UnicodeError) as exc:
        raise Failure('ProcIdentityUnknown', 'supervisor', True) from exc


class LinuxSupervisor:
    """Only launch edge. Two directional control pipes; retained unreaped anchor.

    No tool is launched by __init__ on Darwin, nor by a portable fallback.
    """
    @staticmethod
    def preflight():
        if platform.system() != 'Linux' or sys.version_info < (3, 11):
            fail('UnsupportedSupervisor', 'supervisor')
        required = ('fork', 'setsid', 'pipe2', 'waitid', 'WNOWAIT', 'WEXITED', 'WNOHANG')
        if any(not hasattr(os, name) for name in required) or os.execve not in os.supports_fd:
            fail('UnsupportedSupervisor', 'supervisor')
        import threading
        if threading.active_count() != 1:
            fail('UnsupportedSupervisor', 'supervisor')
        children = LinuxSupervisor.children()
        if children:
            fail('PreexistingChildren', 'supervisor')
        libc = ctypes.CDLL(None, use_errno=True)
        if not hasattr(libc, 'renameat2'):
            fail('UnsupportedSupervisor', 'supervisor')
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            fail('UnsupportedSupervisor', 'supervisor')
        actual = ctypes.c_int()
        if libc.prctl(37, ctypes.byref(actual), 0, 0, 0) != 0 or actual.value != 1:
            fail('UnsupportedSupervisor', 'supervisor')
        proc_stat(os.getpid())

    @staticmethod
    def children():
        path = '/proc/self/task/'+str(os.getpid())+'/children'
        try:
            with open(path, 'rb', buffering=0) as file:
                raw = file.read(LIMITS['child_bytes']+1)
            if len(raw) > LIMITS['child_bytes']:
                fail('SupervisorLimit', 'supervisor', True)
            values = [int(x) for x in raw.split()]
            if len(values) > LIMITS['children'] or len(values) != len(set(values)):
                fail('SupervisorLimit', 'supervisor', True)
            return values
        except (OSError, ValueError) as exc:
            raise Failure('ProcChildrenUnknown', 'supervisor', True) from exc

    def __init__(self, store, budget):
        self.preflight()
        self.store, self.budget = store, budget
        self.sequence, self.poisoned, self.anchor = 0, False, None
        self.records = []
        self.cleanup_end = None

    @staticmethod
    def _wait_control(fd, events, budget):
        # poll has no FD_SETSIZE ceiling and owns no descriptor of its own.
        # Keep the same 100ms/remaining-budget wait, rounding DOWN to millis.
        try:
            waiter = select.poll()
            waiter.register(fd, events)
            waiter.poll(min(100, max(0, int((budget.end-time.monotonic())*1000))))
        except ValueError as exc:
            # A known control-wait refusal must reach the existing pre-GO
            # identity-bound stop/join path, never escape with an open anchor.
            raise Failure('ControlWaitInvalid', 'ready', True) from exc

    @staticmethod
    def _receive(fd, expected, budget):
        data = b''
        while b'\n' not in data:
            budget.check()
            try:
                part = os.read(fd, LIMITS['control']+1-len(data))
            except BlockingIOError:
                LinuxSupervisor._wait_control(fd, select.POLLIN, budget)
                continue
            if not part:
                fail('ControlEOF', 'ready', True)
            data += part
            if len(data) > LIMITS['control']:
                fail('ControlLimit', 'ready', True)
        if data != expected:
            fail('ControlFrame', 'ready', True)
        try:
            extra = os.read(fd, 1)
        except BlockingIOError:
            extra = b''
        if extra:
            fail('ControlFrame', 'ready', True)

    @staticmethod
    def _send(fd, frame, budget):
        if len(frame) > LIMITS['control']:
            fail('ControlLimit', 'ready', True)
        offset = 0
        while offset < len(frame):
            budget.check()
            try:
                written = os.write(fd, frame[offset:])
            except BlockingIOError:
                LinuxSupervisor._wait_control(fd, select.POLLOUT, budget)
                continue
            if written <= 0:
                fail('ControlEOF', 'ready', True)
            offset += written

    @staticmethod
    def _stub(executable, argv, environment, cwd_fd, stdout_fd, stderr_fd,
              ready_write, go_read, ready_read, go_write, nonce, deadline):
        # No caller-supplied callback. Forked stub has one exact pre-exec role.
        try:
            os.close(ready_read)
            os.close(go_write)
            os.umask(0o077)
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGCHLD):
                signal.signal(sig, signal.SIG_DFL)
            os.setsid()
            ready_budget = Budget(deadline)
            LinuxSupervisor._send(ready_write, b'READY '+nonce+b'\n', ready_budget)
            LinuxSupervisor._receive(go_read, b'GO '+nonce+b'\n', ready_budget)
            os.close(ready_write)
            os.close(go_read)
            os.fchdir(cwd_fd)
            null_fd = os.open('/dev/null', os.O_RDONLY | os.O_CLOEXEC)
            os.dup2(null_fd, 0)
            os.dup2(stdout_fd, 1)
            os.dup2(stderr_fd, 2)
            for name in os.listdir('/proc/self/fd'):
                descriptor = int(name)
                if descriptor > 2 and descriptor != executable.fd:
                    try:
                        os.close(descriptor)
                    except OSError as exc:
                        if exc.errno != errno.EBADF:
                            raise
            os.execve(executable.fd, list(argv), environment)
        except BaseException:
            os._exit(126)

    def _members(self, leader, group_ready, scan_end):
        direct = self.children()
        others = []
        for pid in direct:
            if pid == leader:
                continue
            info = proc_stat(pid)
            if not group_ready or info['pgid'] != leader or info['sid'] != leader:
                fail('EscapedChild', 'supervisor', True)
            exited = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if exited is not None:
                os.waitpid(pid, 0)
            else:
                others.append(pid)
        members = []
        with os.scandir('/proc') as entries:
            for entry in entries:
                if time.monotonic() >= scan_end:
                    fail('MembershipTimeout', 'supervisor', True)
                if not entry.name.isdigit():
                    continue
                pid = int(entry.name)
                try:
                    info = proc_stat(pid)
                except Failure:
                    # A process can exit while /proc is scanned. Distinguish
                    # absence from unreadable membership; never assume hidden=0.
                    if not os.path.exists('/proc/'+entry.name):
                        continue
                    raise
                if group_ready and info['pgid'] == leader and pid != leader:
                    if info['sid'] != leader:
                        fail('GroupIdentityChanged', 'supervisor', True)
                    members.append(pid)
                    if len(members) > LIMITS['children']:
                        fail('SupervisorLimit', 'supervisor', True)
        return others, members

    def run_owned(self, argv, executable, cwd, environment, phase_seconds,
                  output_class='ordinary', observe_work=None):
        self.budget.check()
        if self.poisoned or self.anchor is not None:
            fail('SupervisorPoisoned', 'supervisor', True)
        if not argv or argv[0] != executable.path or any(type(a) is not str or len(a.encode()) > LIMITS['argv'] or '\0' in a for a in argv):
            fail('UnsafeArgv')
        executable.recheck(self.budget)
        self.sequence += 1
        sequence = self.sequence
        stdout, stderr = f'commands/{sequence:06d}.stdout', f'commands/{sequence:06d}.stderr'
        out_fd = self.store.create(stdout, 'command_stdout')
        err_fd = self.store.create(stderr, 'command_stderr')
        limit = {'ordinary': LIMITS['stdout'], 'git_object': LIMITS['object_stdout'],
                 'git_tree': LIMITS['object_stdout'], 'graph': LIMITS['graph_stdout']}.get(output_class)
        if limit is None:
            fail('UnknownOutputClass')
        phase = self.budget.child(phase_seconds)
        ready = phase.child(LIMITS['ready_seconds'])
        control_fds = []
        try:
            rr, rw = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
            control_fds.extend((rr, rw))
            gr, gw = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
            control_fds.extend((gr, gw))
            nonce = os.urandom(16).hex().encode('ascii')
            leader = os.fork()
        except BaseException as original:
            # No child was created: the owner starts at the FIRST acquired end.
            # Try every release and preserve the original construction failure.
            cleanup_errors = []
            for descriptor in control_fds:
                try:
                    os.close(descriptor)
                except OSError as error:
                    cleanup_errors.append(error.errno)
            for relative in (stdout, stderr):
                try:
                    self.store.abandon(relative)
                except OSError as error:
                    cleanup_errors.append(error.errno)
            if cleanup_errors and hasattr(original, 'add_note'):
                original.add_note('pre-fork descriptor cleanup errors: '+repr(cleanup_errors))
            raise
        if leader == 0:
            self._stub(executable, tuple(argv), environment, cwd.fd, out_fd, err_fd,
                       rw, gr, rr, gw, nonce, ready.end)
            os._exit(126)
        self.anchor = leader
        os.close(rw)
        os.close(gr)
        group_ready, joined, failure, wait_status = False, False, None, None
        signals, anchor_info = [], None
        stopping, killed = None, False
        try:
            anchor_info = proc_stat(leader)
            self._receive(rr, b'READY '+nonce+b'\n', ready)
            actual = proc_stat(leader)
            if actual['start'] != anchor_info['start'] or actual['ppid'] != os.getpid() or actual['pgid'] != leader or actual['sid'] != leader or os.getpgid(leader) != leader or os.getsid(leader) != leader:
                fail('GroupIdentityUnknown', 'ready', True)
            group_ready = True
            self._send(gw, b'GO '+nonce+b'\n', ready)
        except (Failure, OSError) as exc:
            failure = exc.code if isinstance(exc, Failure) else 'ControlIO'
            stopping = time.monotonic()
        finally:
            os.close(rr)
            os.close(gw)

        def send(sig):
            actual = proc_stat(leader)
            if anchor_info is None or actual['start'] != anchor_info['start'] or actual['ppid'] != os.getpid():
                fail('AnchorIdentityUnknown', 'supervisor', True)
            if group_ready and (actual['pgid'] != leader or actual['sid'] != leader):
                fail('GroupIdentityUnknown', 'supervisor', True)
            try:
                if group_ready:
                    os.killpg(leader, sig)
                else:
                    os.kill(leader, sig)  # retained direct PID, never guessed group
                outcome = 'sent'
            except ProcessLookupError:
                outcome = 'gone'
            except OSError:
                outcome = 'error'
            signals.append((signal.Signals(sig).name, outcome))

        try:
            while True:
                now = time.monotonic()
                if self.budget.cancelled[0]:
                    failure = failure or 'Cancelled'
                if now >= phase.end:
                    failure = failure or 'CommandTimeout'
                if failure and stopping is None:
                    stopping = now
                scan_end = stopping+LIMITS['shutdown_seconds'] if stopping is not None else phase.end
                observed = os.waitid(os.P_PID, leader, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                try:
                    direct, members = self._members(leader, group_ready, scan_end)
                    if os.fstat(out_fd).st_size > limit or os.fstat(err_fd).st_size > LIMITS['stderr']:
                        failure = failure or 'OutputLimit'
                    if observe_work and failure is None:
                        observe_work()
                    if observed is not None and (direct or members):
                        failure = failure or 'DescendantsAfterLeader'
                except (Failure, OSError) as exc:
                    failure = failure or (exc.code if isinstance(exc, Failure) else 'MembershipUnknown')
                    direct, members = [None], [None]
                if observed is not None and not direct and not members:
                    # No surviving descendant can fork. Anchor is reaped LAST.
                    _, wait_status = os.waitpid(leader, 0)
                    self.anchor = None
                    joined = True
                    break
                if failure and stopping is None:
                    stopping = now
                if stopping is not None:
                    if not signals:
                        send(signal.SIGTERM)
                    if now-stopping >= LIMITS['term_seconds'] and not killed:
                        send(signal.SIGKILL)
                        killed = True
                    if now-stopping >= min(LIMITS['shutdown_seconds'], LIMITS['term_seconds']+LIMITS['kill_seconds']):
                        failure = 'CleanupUnconfirmed'
                        break
                time.sleep(LIMITS['monitor_ms']/1000)
        except (Failure, OSError, ChildProcessError) as exc:
            failure = exc.code if isinstance(exc, Failure) else 'WaitUnconfirmed'
        if stopping is not None:
            self.cleanup_end = stopping+LIMITS['shutdown_seconds']
            self.store.budget = Budget(self.cleanup_end)
        if not joined:
            self.poisoned = True
            self.store.abandon(stdout)
            self.store.abandon(stderr)
        else:
            self.store.finish(stdout, complete=failure is None)
            self.store.finish(stderr, complete=failure is None)
        code = os.waitstatus_to_exitcode(wait_status) if wait_status is not None else None
        result = CommandResult(sequence, tuple(argv), stdout, stderr,
                               code if code is None or code >= 0 else None,
                               -code if code is not None and code < 0 else None,
                               joined, failure, tuple(signals))
        self.records.append(result)
        self.store.json(f'commands/{sequence:06d}.json', result.as_json(), 'command_record')
        return result


def git_oid(kind, raw):
    if kind not in ('blob', 'tree', 'commit'):
        fail('GitObjectType')
    return hashlib.sha1(kind.encode()+b' '+str(len(raw)).encode()+b'\0'+raw).hexdigest()


def parse_commit(raw):
    try:
        headers = raw.split(b'\n\n', 1)[0].split(b'\n')
        tree_lines = [x[5:] for x in headers if x.startswith(b'tree ')]
        committers = [x for x in headers if x.startswith(b'committer ')]
        if len(tree_lines) != 1 or len(committers) != 1:
            fail('GitCommitShape')
        tree = tree_lines[0].decode('ascii')
        stamp = int(committers[0].rsplit(b' ', 2)[1])
        if not re.fullmatch(OID, tree) or stamp < 0 or stamp > 2**63-1:
            fail('GitCommitShape')
        return tree, stamp
    except (ValueError, IndexError, UnicodeError) as exc:
        raise Failure('GitCommitShape') from exc


def parse_tree(raw):
    entries, seen, folded, offset = [], set(), set(), 0
    while offset < len(raw):
        space = raw.find(b' ', offset)
        nul = raw.find(b'\0', space+1)
        if space < 0 or nul < 0 or nul+21 > len(raw):
            fail('GitTreeShape')
        try:
            mode, name = raw[offset:space].decode('ascii'), raw[space+1:nul].decode('utf-8')
        except UnicodeError as exc:
            raise Failure('GitTreeShape') from exc
        if mode not in ('40000', '100644', '100755', '120000', '160000') or '/' in name:
            fail('GitTreeMode')
        relative_path(name)
        fold = unicodedata.normalize('NFC', name).casefold()
        if name in seen or fold in folded:
            fail('GitTreeCollision')
        seen.add(name)
        folded.add(fold)
        entries.append((name, mode, raw[nul+1:nul+21].hex()))
        if len(entries) > LIMITS['source_files']:
            fail('CountLimit')
        offset = nul+21
    return entries


def _source_path(path):
    relative_path(path)
    if path not in ('go.mod', 'go.sum') and not any(path.startswith(x+'/') for x in ('cmd', 'internal', 'sdk')):
        fail('MissingSourceClosure')
    if any(part in ('.git', '.env', '.gocache', '.gomodcache', 'refactoring') for part in path.split('/')):
        fail('ForbiddenSource')
    return path


def source_digest(files):
    if type(files) is not list or not files or any(type(x) is not dict or not {'path', 'mode', 'size', 'sha256'} <= set(x) for x in files):
        fail('SourceManifest')
    digest = hashlib.sha256(b'corsa.t5g.v2.source\0')
    last, total = None, 0
    for entry in sorted(files, key=lambda x: x['path']):
        path = _source_path(entry['path']).encode('utf-8')
        if last == path or entry['mode'] not in ('100644', '100755') or not re.fullmatch(HASH, entry['sha256']):
            fail('SourceManifest')
        total += entry['size']
        if not 0 <= entry['size'] <= LIMITS['source_file'] or total > LIMITS['source_total']:
            fail('SourceLimit')
        digest.update(len(path).to_bytes(8, 'big')+path+entry['mode'].encode('ascii')+
                      entry['size'].to_bytes(8, 'big')+bytes.fromhex(entry['sha256']))
        last = path
    return digest.hexdigest()


class GitObjects:
    """CLI always uses this read-only real Git provider; no fixture flag/env."""
    def __init__(self, supervisor, executable, directory, cwd, environment, budget):
        self.supervisor, self.executable, self.directory = supervisor, executable, directory
        self.cwd, self.environment, self.budget = cwd, environment, budget
        self._audit_config()

    def _audit_config(self):
        for path in ('objects/info/alternates', 'objects/info/http-alternates', 'info/grafts', 'shallow', 'commondir'):
            try:
                fd = self.directory.file(path)
            except FileNotFoundError:
                continue
            else:
                os.close(fd)
                fail('UnsupportedGitStorage')
        try:
            with self.directory.directory('refs/replace') as cap:
                if cap.names():
                    fail('UnsupportedGitStorage')
        except FileNotFoundError:
            pass
        fd = self.directory.file('config')
        try:
            raw = read_fd(fd, MIB, self.budget)
        finally:
            os.close(fd)
        parser = configparser.ConfigParser(interpolation=None, strict=True)
        try:
            parser.read_string(raw.decode('utf-8'))
        except (configparser.Error, UnicodeError) as exc:
            raise Failure('GitConfigUnsupported') from exc
        # No config content is emitted. Remote/user identity data is ignored by
        # fixed read-only builtins; includes, lazy fetch and semantic extensions
        # cannot influence selected object interpretation.
        for section in parser.sections():
            lower = section.lower()
            if lower.startswith(('include', 'filter ', 'alias', 'credential', 'url ', 'submodule ')):
                fail('GitConfigUnsupported')
            if lower == 'extensions' and any(k != 'objectformat' for k in parser[section]):
                fail('GitConfigUnsupported')
        if parser.get('core', 'repositoryformatversion', fallback='0') != '0':
            fail('GitConfigUnsupported')
        if parser.get('extensions', 'objectformat', fallback='sha1') != 'sha1':
            fail('GitObjectFormatUnsupported')
        for key in ('fsmonitor', 'sshcommand', 'gitproxy', 'alternateRefsCommand', 'hookspath'):
            if parser.has_option('core', key):
                fail('GitConfigUnsupported')

    def object(self, oid, kind):
        if not re.fullmatch(OID, oid) or kind not in ('blob', 'tree', 'commit'):
            fail('GitObjectIdentity')
        self.budget.check()
        argv = (self.executable.path, '--no-pager', '--no-replace-objects',
                '--git-dir='+self.directory.label, '-c', 'gc.auto=0',
                '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null',
                '-c', 'credential.helper=', 'cat-file', kind, oid)
        result = self.supervisor.run_owned(argv, self.executable, self.cwd, self.environment,
                                           min(LIMITS['command_seconds'], max(0, self.budget.end-time.monotonic())), 'git_object')
        require_command(result)
        fd = self.supervisor.store.root.file(result.stdout)
        try:
            raw = read_fd(fd, LIMITS['object_stdout'], self.budget)
        finally:
            os.close(fd)
        if git_oid(kind, raw) != oid:
            fail('GitObjectIdentity')
        # Raw output and its immutable command reference remain retained. Git
        # transfers have their larger per-command input bound but still consume
        # the pair's finite retained-evidence budget; never delete required bytes
        # merely because the object identity was successfully checked.
        self.supervisor.store.json(f'git/{result.sequence:06d}.json',
            {'oid': oid, 'type': kind, 'size': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}, 'source')
        return raw

    def commit(self, oid):
        return parse_commit(self.object(oid, 'commit'))

    def path(self, tree, path):
        relative_path(path)
        parts = path.split('/')
        for number, part in enumerate(parts):
            entries = {name: (mode, oid) for name, mode, oid in parse_tree(self.object(tree, 'tree'))}
            if part not in entries:
                return None
            mode, oid = entries[part]
            if number == len(parts)-1:
                return mode, oid
            if mode != '40000':
                fail('GitTreePath')
            tree = oid
        fail('GitTreePath')


def require_command(result):
    if not result.joined:
        fail('CleanupUnconfirmed', 'command', True)
    if result.failure:
        fail(result.failure, 'command', True)
    if result.exit_code != 0 or result.term_signal is not None:
        fail('CommandFailed', 'command')


class SourcePlan:
    def __init__(self, provider, request, budget):
        self.provider, self.request, self.budget = provider, request, budget
        tree, self.epoch = provider.commit(request['git']['base_commit'])
        if tree != request['git']['base_tree']:
            fail('BaseTreeMismatch')
        self.tree, self.entries, self.origins = tree, {}, []
        self._walk(tree, '')
        for overlay in request['overlay']:
            self._apply(overlay)
        if 'go.mod' not in self.entries or 'go.sum' not in self.entries:
            fail('MissingSourceClosure')
        self._collisions()

    def _walk(self, tree, prefix):
        for name, mode, oid in parse_tree(self.provider.object(tree, 'tree')):
            path = prefix+name
            if not prefix and name not in ('go.mod', 'go.sum', 'cmd', 'internal', 'sdk'):
                continue
            if mode == '40000':
                if path in ('go.mod', 'go.sum'):
                    fail('GitTreeMode')
                if len(path.split('/')) >= LIMITS['path_depth']:
                    fail('ValidationLimit')
                self._walk(oid, path+'/')
            else:
                _source_path(path)
                if mode not in ('100644', '100755'):
                    fail('GitTreeMode')
                self.entries[path] = (mode, oid, {'kind': 'versioned', 'commit': self.request['git']['base_commit'],
                    'tree': self.tree, 'path': path, 'blob': oid,
                    'staged_capture_manifest_sha256': None, 'role': None})
                if len(self.entries) > LIMITS['source_files']:
                    fail('SourceLimit')

    def _binding(self, entry):
        if entry is None:
            return None
        mode, oid, _ = entry
        raw = self.provider.object(oid, 'blob')
        if len(raw) > LIMITS['source_file']:
            fail('SourceLimit')
        return {'oid': oid, 'sha256': hashlib.sha256(raw).hexdigest(), 'size': len(raw), 'mode': mode}

    def _apply(self, overlay):
        validate('overlay', overlay)
        path = _source_path(overlay['path'])
        if self._binding(self.entries.get(path)) != overlay['before']:
            fail('OverlayBeforeMismatch')
        origin = overlay['origin']
        if origin['kind'] == 'versioned':
            tree, _ = self.provider.commit(origin['commit'])
            if tree != origin['tree']:
                fail('OriginTreeMismatch')
            actual = self.provider.path(tree, origin['path'])
            expected = (overlay['after']['mode'], overlay['after']['oid']) if overlay['after'] else None
            if actual != expected:
                fail('OriginPathMismatch')
        if overlay['after'] is None:
            self.entries.pop(path)
        else:
            after = overlay['after']
            entry = (after['mode'], after['oid'], origin)
            if self._binding(entry) != after:
                fail('OverlayAfterMismatch')
            self.entries[path] = entry
        self.origins.append(origin)

    def _collisions(self):
        seen = {}
        for path in sorted(self.entries):
            _source_path(path)
            parts = path.split('/')
            for n in range(1, len(parts)+1):
                parent = '/'.join(parts[:n])
                fold = unicodedata.normalize('NFC', parent).casefold()
                typ = 'file' if n == len(parts) else 'dir'
                old = seen.get(fold)
                if old is not None and old != (parent, typ):
                    fail('SourceCollision')
                seen[fold] = (parent, typ)

    def materialize(self, destination):
        manifest, total = [], 0
        for path in sorted(self.entries):
            self.budget.check()
            mode, oid, origin = self.entries[path]
            raw = self.provider.object(oid, 'blob')
            total += len(raw)
            if len(raw) > LIMITS['source_file'] or total > LIMITS['source_total']:
                fail('SourceLimit')
            fd = destination.file(path, create=True)
            try:
                _write_all(fd, raw, self.budget)
                os.fchmod(fd, 0o755 if mode == '100755' else 0o644)
                digest, st = hash_fd(fd, LIMITS['source_file'], self.budget)
                if digest != hashlib.sha256(raw).hexdigest() or st.st_size != len(raw):
                    fail('InputMutation', 'source', True)
            finally:
                os.close(fd)
            manifest.append({'path': path, 'size': len(raw), 'sha256': digest,
                             'mode': mode, 'blob': oid, 'origin': origin})
        value = {'schema_version': 2, 'base_commit': self.request['git']['base_commit'],
                 'base_tree': self.tree, 'epoch': self.epoch, 'files': manifest}
        validate('source', value)
        return value


# Pinned Go1.27.2 Unicode17.0.0 data/semantics; no Python casefold/category
# authority for module ZIP names. unicode/tables.go SHA256:
# ac0e5d5d1b58ed4b02d7cb4a0085bdefbdb4f7aae6fadddbc9477f8463da8bd8
# Copyright 2009 The Go Authors.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#    * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#    * Neither the name of Google LLC nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
GO_UNICODE_VERSION = "17.0.0"
GO_UNICODE_TABLES_SHA256 = "ac0e5d5d1b58ed4b02d7cb4a0085bdefbdb4f7aae6fadddbc9477f8463da8bd8"
GO_LETTER_RANGES = (
    (0x41, 0x5A, 1),
    (0x61, 0x7A, 1),
    (0xAA, 0xB5, 11),
    (0xBA, 0xC0, 6),
    (0xC1, 0xD6, 1),
    (0xD8, 0xF6, 1),
    (0xF8, 0x2C1, 1),
    (0x2C6, 0x2D1, 1),
    (0x2E0, 0x2E4, 1),
    (0x2EC, 0x2EE, 2),
    (0x370, 0x374, 1),
    (0x376, 0x377, 1),
    (0x37A, 0x37D, 1),
    (0x37F, 0x386, 7),
    (0x388, 0x38A, 1),
    (0x38C, 0x38E, 2),
    (0x38F, 0x3A1, 1),
    (0x3A3, 0x3F5, 1),
    (0x3F7, 0x481, 1),
    (0x48A, 0x52F, 1),
    (0x531, 0x556, 1),
    (0x559, 0x560, 7),
    (0x561, 0x588, 1),
    (0x5D0, 0x5EA, 1),
    (0x5EF, 0x5F2, 1),
    (0x620, 0x64A, 1),
    (0x66E, 0x66F, 1),
    (0x671, 0x6D3, 1),
    (0x6D5, 0x6E5, 16),
    (0x6E6, 0x6EE, 8),
    (0x6EF, 0x6FA, 11),
    (0x6FB, 0x6FC, 1),
    (0x6FF, 0x710, 17),
    (0x712, 0x72F, 1),
    (0x74D, 0x7A5, 1),
    (0x7B1, 0x7CA, 25),
    (0x7CB, 0x7EA, 1),
    (0x7F4, 0x7F5, 1),
    (0x7FA, 0x800, 6),
    (0x801, 0x815, 1),
    (0x81A, 0x824, 10),
    (0x828, 0x840, 24),
    (0x841, 0x858, 1),
    (0x860, 0x86A, 1),
    (0x870, 0x887, 1),
    (0x889, 0x88F, 1),
    (0x8A0, 0x8C9, 1),
    (0x904, 0x939, 1),
    (0x93D, 0x950, 19),
    (0x958, 0x961, 1),
    (0x971, 0x980, 1),
    (0x985, 0x98C, 1),
    (0x98F, 0x990, 1),
    (0x993, 0x9A8, 1),
    (0x9AA, 0x9B0, 1),
    (0x9B2, 0x9B6, 4),
    (0x9B7, 0x9B9, 1),
    (0x9BD, 0x9CE, 17),
    (0x9DC, 0x9DD, 1),
    (0x9DF, 0x9E1, 1),
    (0x9F0, 0x9F1, 1),
    (0x9FC, 0xA05, 9),
    (0xA06, 0xA0A, 1),
    (0xA0F, 0xA10, 1),
    (0xA13, 0xA28, 1),
    (0xA2A, 0xA30, 1),
    (0xA32, 0xA33, 1),
    (0xA35, 0xA36, 1),
    (0xA38, 0xA39, 1),
    (0xA59, 0xA5C, 1),
    (0xA5E, 0xA72, 20),
    (0xA73, 0xA74, 1),
    (0xA85, 0xA8D, 1),
    (0xA8F, 0xA91, 1),
    (0xA93, 0xAA8, 1),
    (0xAAA, 0xAB0, 1),
    (0xAB2, 0xAB3, 1),
    (0xAB5, 0xAB9, 1),
    (0xABD, 0xAD0, 19),
    (0xAE0, 0xAE1, 1),
    (0xAF9, 0xB05, 12),
    (0xB06, 0xB0C, 1),
    (0xB0F, 0xB10, 1),
    (0xB13, 0xB28, 1),
    (0xB2A, 0xB30, 1),
    (0xB32, 0xB33, 1),
    (0xB35, 0xB39, 1),
    (0xB3D, 0xB5C, 31),
    (0xB5D, 0xB5F, 2),
    (0xB60, 0xB61, 1),
    (0xB71, 0xB83, 18),
    (0xB85, 0xB8A, 1),
    (0xB8E, 0xB90, 1),
    (0xB92, 0xB95, 1),
    (0xB99, 0xB9A, 1),
    (0xB9C, 0xB9E, 2),
    (0xB9F, 0xBA3, 4),
    (0xBA4, 0xBA8, 4),
    (0xBA9, 0xBAA, 1),
    (0xBAE, 0xBB9, 1),
    (0xBD0, 0xC05, 53),
    (0xC06, 0xC0C, 1),
    (0xC0E, 0xC10, 1),
    (0xC12, 0xC28, 1),
    (0xC2A, 0xC39, 1),
    (0xC3D, 0xC58, 27),
    (0xC59, 0xC5A, 1),
    (0xC5C, 0xC5D, 1),
    (0xC60, 0xC61, 1),
    (0xC80, 0xC85, 5),
    (0xC86, 0xC8C, 1),
    (0xC8E, 0xC90, 1),
    (0xC92, 0xCA8, 1),
    (0xCAA, 0xCB3, 1),
    (0xCB5, 0xCB9, 1),
    (0xCBD, 0xCDC, 31),
    (0xCDD, 0xCDE, 1),
    (0xCE0, 0xCE1, 1),
    (0xCF1, 0xCF2, 1),
    (0xD04, 0xD0C, 1),
    (0xD0E, 0xD10, 1),
    (0xD12, 0xD3A, 1),
    (0xD3D, 0xD4E, 17),
    (0xD54, 0xD56, 1),
    (0xD5F, 0xD61, 1),
    (0xD7A, 0xD7F, 1),
    (0xD85, 0xD96, 1),
    (0xD9A, 0xDB1, 1),
    (0xDB3, 0xDBB, 1),
    (0xDBD, 0xDC0, 3),
    (0xDC1, 0xDC6, 1),
    (0xE01, 0xE30, 1),
    (0xE32, 0xE33, 1),
    (0xE40, 0xE46, 1),
    (0xE81, 0xE82, 1),
    (0xE84, 0xE86, 2),
    (0xE87, 0xE8A, 1),
    (0xE8C, 0xEA3, 1),
    (0xEA5, 0xEA7, 2),
    (0xEA8, 0xEB0, 1),
    (0xEB2, 0xEB3, 1),
    (0xEBD, 0xEC0, 3),
    (0xEC1, 0xEC4, 1),
    (0xEC6, 0xEDC, 22),
    (0xEDD, 0xEDF, 1),
    (0xF00, 0xF40, 64),
    (0xF41, 0xF47, 1),
    (0xF49, 0xF6C, 1),
    (0xF88, 0xF8C, 1),
    (0x1000, 0x102A, 1),
    (0x103F, 0x1050, 17),
    (0x1051, 0x1055, 1),
    (0x105A, 0x105D, 1),
    (0x1061, 0x1065, 4),
    (0x1066, 0x106E, 8),
    (0x106F, 0x1070, 1),
    (0x1075, 0x1081, 1),
    (0x108E, 0x10A0, 18),
    (0x10A1, 0x10C5, 1),
    (0x10C7, 0x10CD, 6),
    (0x10D0, 0x10FA, 1),
    (0x10FC, 0x1248, 1),
    (0x124A, 0x124D, 1),
    (0x1250, 0x1256, 1),
    (0x1258, 0x125A, 2),
    (0x125B, 0x125D, 1),
    (0x1260, 0x1288, 1),
    (0x128A, 0x128D, 1),
    (0x1290, 0x12B0, 1),
    (0x12B2, 0x12B5, 1),
    (0x12B8, 0x12BE, 1),
    (0x12C0, 0x12C2, 2),
    (0x12C3, 0x12C5, 1),
    (0x12C8, 0x12D6, 1),
    (0x12D8, 0x1310, 1),
    (0x1312, 0x1315, 1),
    (0x1318, 0x135A, 1),
    (0x1380, 0x138F, 1),
    (0x13A0, 0x13F5, 1),
    (0x13F8, 0x13FD, 1),
    (0x1401, 0x166C, 1),
    (0x166F, 0x167F, 1),
    (0x1681, 0x169A, 1),
    (0x16A0, 0x16EA, 1),
    (0x16F1, 0x16F8, 1),
    (0x1700, 0x1711, 1),
    (0x171F, 0x1731, 1),
    (0x1740, 0x1751, 1),
    (0x1760, 0x176C, 1),
    (0x176E, 0x1770, 1),
    (0x1780, 0x17B3, 1),
    (0x17D7, 0x17DC, 5),
    (0x1820, 0x1878, 1),
    (0x1880, 0x1884, 1),
    (0x1887, 0x18A8, 1),
    (0x18AA, 0x18B0, 6),
    (0x18B1, 0x18F5, 1),
    (0x1900, 0x191E, 1),
    (0x1950, 0x196D, 1),
    (0x1970, 0x1974, 1),
    (0x1980, 0x19AB, 1),
    (0x19B0, 0x19C9, 1),
    (0x1A00, 0x1A16, 1),
    (0x1A20, 0x1A54, 1),
    (0x1AA7, 0x1B05, 94),
    (0x1B06, 0x1B33, 1),
    (0x1B45, 0x1B4C, 1),
    (0x1B83, 0x1BA0, 1),
    (0x1BAE, 0x1BAF, 1),
    (0x1BBA, 0x1BE5, 1),
    (0x1C00, 0x1C23, 1),
    (0x1C4D, 0x1C4F, 1),
    (0x1C5A, 0x1C7D, 1),
    (0x1C80, 0x1C8A, 1),
    (0x1C90, 0x1CBA, 1),
    (0x1CBD, 0x1CBF, 1),
    (0x1CE9, 0x1CEC, 1),
    (0x1CEE, 0x1CF3, 1),
    (0x1CF5, 0x1CF6, 1),
    (0x1CFA, 0x1D00, 6),
    (0x1D01, 0x1DBF, 1),
    (0x1E00, 0x1F15, 1),
    (0x1F18, 0x1F1D, 1),
    (0x1F20, 0x1F45, 1),
    (0x1F48, 0x1F4D, 1),
    (0x1F50, 0x1F57, 1),
    (0x1F59, 0x1F5F, 2),
    (0x1F60, 0x1F7D, 1),
    (0x1F80, 0x1FB4, 1),
    (0x1FB6, 0x1FBC, 1),
    (0x1FBE, 0x1FC2, 4),
    (0x1FC3, 0x1FC4, 1),
    (0x1FC6, 0x1FCC, 1),
    (0x1FD0, 0x1FD3, 1),
    (0x1FD6, 0x1FDB, 1),
    (0x1FE0, 0x1FEC, 1),
    (0x1FF2, 0x1FF4, 1),
    (0x1FF6, 0x1FFC, 1),
    (0x2071, 0x207F, 14),
    (0x2090, 0x209C, 1),
    (0x2102, 0x2107, 5),
    (0x210A, 0x2113, 1),
    (0x2115, 0x2119, 4),
    (0x211A, 0x211D, 1),
    (0x2124, 0x212A, 2),
    (0x212B, 0x212D, 1),
    (0x212F, 0x2139, 1),
    (0x213C, 0x213F, 1),
    (0x2145, 0x2149, 1),
    (0x214E, 0x2183, 53),
    (0x2184, 0x2C00, 2684),
    (0x2C01, 0x2CE4, 1),
    (0x2CEB, 0x2CEE, 1),
    (0x2CF2, 0x2CF3, 1),
    (0x2D00, 0x2D25, 1),
    (0x2D27, 0x2D2D, 6),
    (0x2D30, 0x2D67, 1),
    (0x2D6F, 0x2D80, 17),
    (0x2D81, 0x2D96, 1),
    (0x2DA0, 0x2DA6, 1),
    (0x2DA8, 0x2DAE, 1),
    (0x2DB0, 0x2DB6, 1),
    (0x2DB8, 0x2DBE, 1),
    (0x2DC0, 0x2DC6, 1),
    (0x2DC8, 0x2DCE, 1),
    (0x2DD0, 0x2DD6, 1),
    (0x2DD8, 0x2DDE, 1),
    (0x2E2F, 0x3005, 470),
    (0x3006, 0x3031, 43),
    (0x3032, 0x3035, 1),
    (0x303B, 0x303C, 1),
    (0x3041, 0x3096, 1),
    (0x309D, 0x309F, 1),
    (0x30A1, 0x30FA, 1),
    (0x30FC, 0x30FF, 1),
    (0x3105, 0x312F, 1),
    (0x3131, 0x318E, 1),
    (0x31A0, 0x31BF, 1),
    (0x31F0, 0x31FF, 1),
    (0x3400, 0x4DBF, 1),
    (0x4E00, 0xA48C, 1),
    (0xA4D0, 0xA4FD, 1),
    (0xA500, 0xA60C, 1),
    (0xA610, 0xA61F, 1),
    (0xA62A, 0xA62B, 1),
    (0xA640, 0xA66E, 1),
    (0xA67F, 0xA69D, 1),
    (0xA6A0, 0xA6E5, 1),
    (0xA717, 0xA71F, 1),
    (0xA722, 0xA788, 1),
    (0xA78B, 0xA7DC, 1),
    (0xA7F1, 0xA801, 1),
    (0xA803, 0xA805, 1),
    (0xA807, 0xA80A, 1),
    (0xA80C, 0xA822, 1),
    (0xA840, 0xA873, 1),
    (0xA882, 0xA8B3, 1),
    (0xA8F2, 0xA8F7, 1),
    (0xA8FB, 0xA8FD, 2),
    (0xA8FE, 0xA90A, 12),
    (0xA90B, 0xA925, 1),
    (0xA930, 0xA946, 1),
    (0xA960, 0xA97C, 1),
    (0xA984, 0xA9B2, 1),
    (0xA9CF, 0xA9E0, 17),
    (0xA9E1, 0xA9E4, 1),
    (0xA9E6, 0xA9EF, 1),
    (0xA9FA, 0xA9FE, 1),
    (0xAA00, 0xAA28, 1),
    (0xAA40, 0xAA42, 1),
    (0xAA44, 0xAA4B, 1),
    (0xAA60, 0xAA76, 1),
    (0xAA7A, 0xAA7E, 4),
    (0xAA7F, 0xAAAF, 1),
    (0xAAB1, 0xAAB5, 4),
    (0xAAB6, 0xAAB9, 3),
    (0xAABA, 0xAABD, 1),
    (0xAAC0, 0xAAC2, 2),
    (0xAADB, 0xAADD, 1),
    (0xAAE0, 0xAAEA, 1),
    (0xAAF2, 0xAAF4, 1),
    (0xAB01, 0xAB06, 1),
    (0xAB09, 0xAB0E, 1),
    (0xAB11, 0xAB16, 1),
    (0xAB20, 0xAB26, 1),
    (0xAB28, 0xAB2E, 1),
    (0xAB30, 0xAB5A, 1),
    (0xAB5C, 0xAB69, 1),
    (0xAB70, 0xABE2, 1),
    (0xAC00, 0xD7A3, 1),
    (0xD7B0, 0xD7C6, 1),
    (0xD7CB, 0xD7FB, 1),
    (0xF900, 0xFA6D, 1),
    (0xFA70, 0xFAD9, 1),
    (0xFB00, 0xFB06, 1),
    (0xFB13, 0xFB17, 1),
    (0xFB1D, 0xFB1F, 2),
    (0xFB20, 0xFB28, 1),
    (0xFB2A, 0xFB36, 1),
    (0xFB38, 0xFB3C, 1),
    (0xFB3E, 0xFB40, 2),
    (0xFB41, 0xFB43, 2),
    (0xFB44, 0xFB46, 2),
    (0xFB47, 0xFBB1, 1),
    (0xFBD3, 0xFD3D, 1),
    (0xFD50, 0xFD8F, 1),
    (0xFD92, 0xFDC7, 1),
    (0xFDF0, 0xFDFB, 1),
    (0xFE70, 0xFE74, 1),
    (0xFE76, 0xFEFC, 1),
    (0xFF21, 0xFF3A, 1),
    (0xFF41, 0xFF5A, 1),
    (0xFF66, 0xFFBE, 1),
    (0xFFC2, 0xFFC7, 1),
    (0xFFCA, 0xFFCF, 1),
    (0xFFD2, 0xFFD7, 1),
    (0xFFDA, 0xFFDC, 1),
    (0x10000, 0x1000B, 1),
    (0x1000D, 0x10026, 1),
    (0x10028, 0x1003A, 1),
    (0x1003C, 0x1003D, 1),
    (0x1003F, 0x1004D, 1),
    (0x10050, 0x1005D, 1),
    (0x10080, 0x100FA, 1),
    (0x10280, 0x1029C, 1),
    (0x102A0, 0x102D0, 1),
    (0x10300, 0x1031F, 1),
    (0x1032D, 0x10340, 1),
    (0x10342, 0x10349, 1),
    (0x10350, 0x10375, 1),
    (0x10380, 0x1039D, 1),
    (0x103A0, 0x103C3, 1),
    (0x103C8, 0x103CF, 1),
    (0x10400, 0x1049D, 1),
    (0x104B0, 0x104D3, 1),
    (0x104D8, 0x104FB, 1),
    (0x10500, 0x10527, 1),
    (0x10530, 0x10563, 1),
    (0x10570, 0x1057A, 1),
    (0x1057C, 0x1058A, 1),
    (0x1058C, 0x10592, 1),
    (0x10594, 0x10595, 1),
    (0x10597, 0x105A1, 1),
    (0x105A3, 0x105B1, 1),
    (0x105B3, 0x105B9, 1),
    (0x105BB, 0x105BC, 1),
    (0x105C0, 0x105F3, 1),
    (0x10600, 0x10736, 1),
    (0x10740, 0x10755, 1),
    (0x10760, 0x10767, 1),
    (0x10780, 0x10785, 1),
    (0x10787, 0x107B0, 1),
    (0x107B2, 0x107BA, 1),
    (0x10800, 0x10805, 1),
    (0x10808, 0x1080A, 2),
    (0x1080B, 0x10835, 1),
    (0x10837, 0x10838, 1),
    (0x1083C, 0x1083F, 3),
    (0x10840, 0x10855, 1),
    (0x10860, 0x10876, 1),
    (0x10880, 0x1089E, 1),
    (0x108E0, 0x108F2, 1),
    (0x108F4, 0x108F5, 1),
    (0x10900, 0x10915, 1),
    (0x10920, 0x10939, 1),
    (0x10940, 0x10959, 1),
    (0x10980, 0x109B7, 1),
    (0x109BE, 0x109BF, 1),
    (0x10A00, 0x10A10, 16),
    (0x10A11, 0x10A13, 1),
    (0x10A15, 0x10A17, 1),
    (0x10A19, 0x10A35, 1),
    (0x10A60, 0x10A7C, 1),
    (0x10A80, 0x10A9C, 1),
    (0x10AC0, 0x10AC7, 1),
    (0x10AC9, 0x10AE4, 1),
    (0x10B00, 0x10B35, 1),
    (0x10B40, 0x10B55, 1),
    (0x10B60, 0x10B72, 1),
    (0x10B80, 0x10B91, 1),
    (0x10C00, 0x10C48, 1),
    (0x10C80, 0x10CB2, 1),
    (0x10CC0, 0x10CF2, 1),
    (0x10D00, 0x10D23, 1),
    (0x10D4A, 0x10D65, 1),
    (0x10D6F, 0x10D85, 1),
    (0x10E80, 0x10EA9, 1),
    (0x10EB0, 0x10EB1, 1),
    (0x10EC2, 0x10EC7, 1),
    (0x10F00, 0x10F1C, 1),
    (0x10F27, 0x10F30, 9),
    (0x10F31, 0x10F45, 1),
    (0x10F70, 0x10F81, 1),
    (0x10FB0, 0x10FC4, 1),
    (0x10FE0, 0x10FF6, 1),
    (0x11003, 0x11037, 1),
    (0x11071, 0x11072, 1),
    (0x11075, 0x11083, 14),
    (0x11084, 0x110AF, 1),
    (0x110D0, 0x110E8, 1),
    (0x11103, 0x11126, 1),
    (0x11144, 0x11147, 3),
    (0x11150, 0x11172, 1),
    (0x11176, 0x11183, 13),
    (0x11184, 0x111B2, 1),
    (0x111C1, 0x111C4, 1),
    (0x111DA, 0x111DC, 2),
    (0x11200, 0x11211, 1),
    (0x11213, 0x1122B, 1),
    (0x1123F, 0x11240, 1),
    (0x11280, 0x11286, 1),
    (0x11288, 0x1128A, 2),
    (0x1128B, 0x1128D, 1),
    (0x1128F, 0x1129D, 1),
    (0x1129F, 0x112A8, 1),
    (0x112B0, 0x112DE, 1),
    (0x11305, 0x1130C, 1),
    (0x1130F, 0x11310, 1),
    (0x11313, 0x11328, 1),
    (0x1132A, 0x11330, 1),
    (0x11332, 0x11333, 1),
    (0x11335, 0x11339, 1),
    (0x1133D, 0x11350, 19),
    (0x1135D, 0x11361, 1),
    (0x11380, 0x11389, 1),
    (0x1138B, 0x1138E, 3),
    (0x11390, 0x113B5, 1),
    (0x113B7, 0x113D1, 26),
    (0x113D3, 0x11400, 45),
    (0x11401, 0x11434, 1),
    (0x11447, 0x1144A, 1),
    (0x1145F, 0x11461, 1),
    (0x11480, 0x114AF, 1),
    (0x114C4, 0x114C5, 1),
    (0x114C7, 0x11580, 185),
    (0x11581, 0x115AE, 1),
    (0x115D8, 0x115DB, 1),
    (0x11600, 0x1162F, 1),
    (0x11644, 0x11680, 60),
    (0x11681, 0x116AA, 1),
    (0x116B8, 0x11700, 72),
    (0x11701, 0x1171A, 1),
    (0x11740, 0x11746, 1),
    (0x11800, 0x1182B, 1),
    (0x118A0, 0x118DF, 1),
    (0x118FF, 0x11906, 1),
    (0x11909, 0x1190C, 3),
    (0x1190D, 0x11913, 1),
    (0x11915, 0x11916, 1),
    (0x11918, 0x1192F, 1),
    (0x1193F, 0x11941, 2),
    (0x119A0, 0x119A7, 1),
    (0x119AA, 0x119D0, 1),
    (0x119E1, 0x119E3, 2),
    (0x11A00, 0x11A0B, 11),
    (0x11A0C, 0x11A32, 1),
    (0x11A3A, 0x11A50, 22),
    (0x11A5C, 0x11A89, 1),
    (0x11A9D, 0x11AB0, 19),
    (0x11AB1, 0x11AF8, 1),
    (0x11BC0, 0x11BE0, 1),
    (0x11C00, 0x11C08, 1),
    (0x11C0A, 0x11C2E, 1),
    (0x11C40, 0x11C72, 50),
    (0x11C73, 0x11C8F, 1),
    (0x11D00, 0x11D06, 1),
    (0x11D08, 0x11D09, 1),
    (0x11D0B, 0x11D30, 1),
    (0x11D46, 0x11D60, 26),
    (0x11D61, 0x11D65, 1),
    (0x11D67, 0x11D68, 1),
    (0x11D6A, 0x11D89, 1),
    (0x11D98, 0x11DB0, 24),
    (0x11DB1, 0x11DDB, 1),
    (0x11EE0, 0x11EF2, 1),
    (0x11F02, 0x11F04, 2),
    (0x11F05, 0x11F10, 1),
    (0x11F12, 0x11F33, 1),
    (0x11FB0, 0x12000, 80),
    (0x12001, 0x12399, 1),
    (0x12480, 0x12543, 1),
    (0x12F90, 0x12FF0, 1),
    (0x13000, 0x1342F, 1),
    (0x13441, 0x13446, 1),
    (0x13460, 0x143FA, 1),
    (0x14400, 0x14646, 1),
    (0x16100, 0x1611D, 1),
    (0x16800, 0x16A38, 1),
    (0x16A40, 0x16A5E, 1),
    (0x16A70, 0x16ABE, 1),
    (0x16AD0, 0x16AED, 1),
    (0x16B00, 0x16B2F, 1),
    (0x16B40, 0x16B43, 1),
    (0x16B63, 0x16B77, 1),
    (0x16B7D, 0x16B8F, 1),
    (0x16D40, 0x16D6C, 1),
    (0x16E40, 0x16E7F, 1),
    (0x16EA0, 0x16EB8, 1),
    (0x16EBB, 0x16ED3, 1),
    (0x16F00, 0x16F4A, 1),
    (0x16F50, 0x16F93, 67),
    (0x16F94, 0x16F9F, 1),
    (0x16FE0, 0x16FE1, 1),
    (0x16FE3, 0x16FF2, 15),
    (0x16FF3, 0x17000, 13),
    (0x17001, 0x18CD5, 1),
    (0x18CFF, 0x18D1E, 1),
    (0x18D80, 0x18DF2, 1),
    (0x1AFF0, 0x1AFF3, 1),
    (0x1AFF5, 0x1AFFB, 1),
    (0x1AFFD, 0x1AFFE, 1),
    (0x1B000, 0x1B122, 1),
    (0x1B132, 0x1B150, 30),
    (0x1B151, 0x1B152, 1),
    (0x1B155, 0x1B164, 15),
    (0x1B165, 0x1B167, 1),
    (0x1B170, 0x1B2FB, 1),
    (0x1BC00, 0x1BC6A, 1),
    (0x1BC70, 0x1BC7C, 1),
    (0x1BC80, 0x1BC88, 1),
    (0x1BC90, 0x1BC99, 1),
    (0x1D400, 0x1D454, 1),
    (0x1D456, 0x1D49C, 1),
    (0x1D49E, 0x1D49F, 1),
    (0x1D4A2, 0x1D4A5, 3),
    (0x1D4A6, 0x1D4A9, 3),
    (0x1D4AA, 0x1D4AC, 1),
    (0x1D4AE, 0x1D4B9, 1),
    (0x1D4BB, 0x1D4BD, 2),
    (0x1D4BE, 0x1D4C3, 1),
    (0x1D4C5, 0x1D505, 1),
    (0x1D507, 0x1D50A, 1),
    (0x1D50D, 0x1D514, 1),
    (0x1D516, 0x1D51C, 1),
    (0x1D51E, 0x1D539, 1),
    (0x1D53B, 0x1D53E, 1),
    (0x1D540, 0x1D544, 1),
    (0x1D546, 0x1D54A, 4),
    (0x1D54B, 0x1D550, 1),
    (0x1D552, 0x1D6A5, 1),
    (0x1D6A8, 0x1D6C0, 1),
    (0x1D6C2, 0x1D6DA, 1),
    (0x1D6DC, 0x1D6FA, 1),
    (0x1D6FC, 0x1D714, 1),
    (0x1D716, 0x1D734, 1),
    (0x1D736, 0x1D74E, 1),
    (0x1D750, 0x1D76E, 1),
    (0x1D770, 0x1D788, 1),
    (0x1D78A, 0x1D7A8, 1),
    (0x1D7AA, 0x1D7C2, 1),
    (0x1D7C4, 0x1D7CB, 1),
    (0x1DF00, 0x1DF1E, 1),
    (0x1DF25, 0x1DF2A, 1),
    (0x1E030, 0x1E06D, 1),
    (0x1E100, 0x1E12C, 1),
    (0x1E137, 0x1E13D, 1),
    (0x1E14E, 0x1E290, 322),
    (0x1E291, 0x1E2AD, 1),
    (0x1E2C0, 0x1E2EB, 1),
    (0x1E4D0, 0x1E4EB, 1),
    (0x1E5D0, 0x1E5ED, 1),
    (0x1E5F0, 0x1E6C0, 208),
    (0x1E6C1, 0x1E6DE, 1),
    (0x1E6E0, 0x1E6E2, 1),
    (0x1E6E4, 0x1E6E5, 1),
    (0x1E6E7, 0x1E6ED, 1),
    (0x1E6F0, 0x1E6F4, 1),
    (0x1E6FE, 0x1E6FF, 1),
    (0x1E7E0, 0x1E7E6, 1),
    (0x1E7E8, 0x1E7EB, 1),
    (0x1E7ED, 0x1E7EE, 1),
    (0x1E7F0, 0x1E7FE, 1),
    (0x1E800, 0x1E8C4, 1),
    (0x1E900, 0x1E943, 1),
    (0x1E94B, 0x1EE00, 1205),
    (0x1EE01, 0x1EE03, 1),
    (0x1EE05, 0x1EE1F, 1),
    (0x1EE21, 0x1EE22, 1),
    (0x1EE24, 0x1EE27, 3),
    (0x1EE29, 0x1EE32, 1),
    (0x1EE34, 0x1EE37, 1),
    (0x1EE39, 0x1EE3B, 2),
    (0x1EE42, 0x1EE47, 5),
    (0x1EE49, 0x1EE4D, 2),
    (0x1EE4E, 0x1EE4F, 1),
    (0x1EE51, 0x1EE52, 1),
    (0x1EE54, 0x1EE57, 3),
    (0x1EE59, 0x1EE61, 2),
    (0x1EE62, 0x1EE64, 2),
    (0x1EE67, 0x1EE6A, 1),
    (0x1EE6C, 0x1EE72, 1),
    (0x1EE74, 0x1EE77, 1),
    (0x1EE79, 0x1EE7C, 1),
    (0x1EE7E, 0x1EE80, 2),
    (0x1EE81, 0x1EE89, 1),
    (0x1EE8B, 0x1EE9B, 1),
    (0x1EEA1, 0x1EEA3, 1),
    (0x1EEA5, 0x1EEA9, 1),
    (0x1EEAB, 0x1EEBB, 1),
    (0x20000, 0x2A6DF, 1),
    (0x2A700, 0x2B81D, 1),
    (0x2B820, 0x2CEAD, 1),
    (0x2CEB0, 0x2EBE0, 1),
    (0x2EBF0, 0x2EE5D, 1),
    (0x2F800, 0x2FA1D, 1),
    (0x30000, 0x3134A, 1),
    (0x31350, 0x33479, 1),
)
GO_CASE_RANGES = (
    (0x41, 0x5A, 0, 32),
    (0x61, 0x7A, -32, 0),
    (0xB5, 0xB5, 743, 0),
    (0xC0, 0xD6, 0, 32),
    (0xD8, 0xDE, 0, 32),
    (0xE0, 0xF6, -32, 0),
    (0xF8, 0xFE, -32, 0),
    (0xFF, 0xFF, 121, 0),
    (0x100, 0x12F, None, None),
    (0x130, 0x130, 0, -199),
    (0x131, 0x131, -232, 0),
    (0x132, 0x137, None, None),
    (0x139, 0x148, None, None),
    (0x14A, 0x177, None, None),
    (0x178, 0x178, 0, -121),
    (0x179, 0x17E, None, None),
    (0x17F, 0x17F, -300, 0),
    (0x180, 0x180, 195, 0),
    (0x181, 0x181, 0, 210),
    (0x182, 0x185, None, None),
    (0x186, 0x186, 0, 206),
    (0x187, 0x188, None, None),
    (0x189, 0x18A, 0, 205),
    (0x18B, 0x18C, None, None),
    (0x18E, 0x18E, 0, 79),
    (0x18F, 0x18F, 0, 202),
    (0x190, 0x190, 0, 203),
    (0x191, 0x192, None, None),
    (0x193, 0x193, 0, 205),
    (0x194, 0x194, 0, 207),
    (0x195, 0x195, 97, 0),
    (0x196, 0x196, 0, 211),
    (0x197, 0x197, 0, 209),
    (0x198, 0x199, None, None),
    (0x19A, 0x19A, 163, 0),
    (0x19B, 0x19B, 42561, 0),
    (0x19C, 0x19C, 0, 211),
    (0x19D, 0x19D, 0, 213),
    (0x19E, 0x19E, 130, 0),
    (0x19F, 0x19F, 0, 214),
    (0x1A0, 0x1A5, None, None),
    (0x1A6, 0x1A6, 0, 218),
    (0x1A7, 0x1A8, None, None),
    (0x1A9, 0x1A9, 0, 218),
    (0x1AC, 0x1AD, None, None),
    (0x1AE, 0x1AE, 0, 218),
    (0x1AF, 0x1B0, None, None),
    (0x1B1, 0x1B2, 0, 217),
    (0x1B3, 0x1B6, None, None),
    (0x1B7, 0x1B7, 0, 219),
    (0x1B8, 0x1B9, None, None),
    (0x1BC, 0x1BD, None, None),
    (0x1BF, 0x1BF, 56, 0),
    (0x1C4, 0x1C4, 0, 2),
    (0x1C5, 0x1C5, -1, 1),
    (0x1C6, 0x1C6, -2, 0),
    (0x1C7, 0x1C7, 0, 2),
    (0x1C8, 0x1C8, -1, 1),
    (0x1C9, 0x1C9, -2, 0),
    (0x1CA, 0x1CA, 0, 2),
    (0x1CB, 0x1CB, -1, 1),
    (0x1CC, 0x1CC, -2, 0),
    (0x1CD, 0x1DC, None, None),
    (0x1DD, 0x1DD, -79, 0),
    (0x1DE, 0x1EF, None, None),
    (0x1F1, 0x1F1, 0, 2),
    (0x1F2, 0x1F2, -1, 1),
    (0x1F3, 0x1F3, -2, 0),
    (0x1F4, 0x1F5, None, None),
    (0x1F6, 0x1F6, 0, -97),
    (0x1F7, 0x1F7, 0, -56),
    (0x1F8, 0x21F, None, None),
    (0x220, 0x220, 0, -130),
    (0x222, 0x233, None, None),
    (0x23A, 0x23A, 0, 10795),
    (0x23B, 0x23C, None, None),
    (0x23D, 0x23D, 0, -163),
    (0x23E, 0x23E, 0, 10792),
    (0x23F, 0x240, 10815, 0),
    (0x241, 0x242, None, None),
    (0x243, 0x243, 0, -195),
    (0x244, 0x244, 0, 69),
    (0x245, 0x245, 0, 71),
    (0x246, 0x24F, None, None),
    (0x250, 0x250, 10783, 0),
    (0x251, 0x251, 10780, 0),
    (0x252, 0x252, 10782, 0),
    (0x253, 0x253, -210, 0),
    (0x254, 0x254, -206, 0),
    (0x256, 0x257, -205, 0),
    (0x259, 0x259, -202, 0),
    (0x25B, 0x25B, -203, 0),
    (0x25C, 0x25C, 42319, 0),
    (0x260, 0x260, -205, 0),
    (0x261, 0x261, 42315, 0),
    (0x263, 0x263, -207, 0),
    (0x264, 0x264, 42343, 0),
    (0x265, 0x265, 42280, 0),
    (0x266, 0x266, 42308, 0),
    (0x268, 0x268, -209, 0),
    (0x269, 0x269, -211, 0),
    (0x26A, 0x26A, 42308, 0),
    (0x26B, 0x26B, 10743, 0),
    (0x26C, 0x26C, 42305, 0),
    (0x26F, 0x26F, -211, 0),
    (0x271, 0x271, 10749, 0),
    (0x272, 0x272, -213, 0),
    (0x275, 0x275, -214, 0),
    (0x27D, 0x27D, 10727, 0),
    (0x280, 0x280, -218, 0),
    (0x282, 0x282, 42307, 0),
    (0x283, 0x283, -218, 0),
    (0x287, 0x287, 42282, 0),
    (0x288, 0x288, -218, 0),
    (0x289, 0x289, -69, 0),
    (0x28A, 0x28B, -217, 0),
    (0x28C, 0x28C, -71, 0),
    (0x292, 0x292, -219, 0),
    (0x29D, 0x29D, 42261, 0),
    (0x29E, 0x29E, 42258, 0),
    (0x345, 0x345, 84, 0),
    (0x370, 0x373, None, None),
    (0x376, 0x377, None, None),
    (0x37B, 0x37D, 130, 0),
    (0x37F, 0x37F, 0, 116),
    (0x386, 0x386, 0, 38),
    (0x388, 0x38A, 0, 37),
    (0x38C, 0x38C, 0, 64),
    (0x38E, 0x38F, 0, 63),
    (0x391, 0x3A1, 0, 32),
    (0x3A3, 0x3AB, 0, 32),
    (0x3AC, 0x3AC, -38, 0),
    (0x3AD, 0x3AF, -37, 0),
    (0x3B1, 0x3C1, -32, 0),
    (0x3C2, 0x3C2, -31, 0),
    (0x3C3, 0x3CB, -32, 0),
    (0x3CC, 0x3CC, -64, 0),
    (0x3CD, 0x3CE, -63, 0),
    (0x3CF, 0x3CF, 0, 8),
    (0x3D0, 0x3D0, -62, 0),
    (0x3D1, 0x3D1, -57, 0),
    (0x3D5, 0x3D5, -47, 0),
    (0x3D6, 0x3D6, -54, 0),
    (0x3D7, 0x3D7, -8, 0),
    (0x3D8, 0x3EF, None, None),
    (0x3F0, 0x3F0, -86, 0),
    (0x3F1, 0x3F1, -80, 0),
    (0x3F2, 0x3F2, 7, 0),
    (0x3F3, 0x3F3, -116, 0),
    (0x3F4, 0x3F4, 0, -60),
    (0x3F5, 0x3F5, -96, 0),
    (0x3F7, 0x3F8, None, None),
    (0x3F9, 0x3F9, 0, -7),
    (0x3FA, 0x3FB, None, None),
    (0x3FD, 0x3FF, 0, -130),
    (0x400, 0x40F, 0, 80),
    (0x410, 0x42F, 0, 32),
    (0x430, 0x44F, -32, 0),
    (0x450, 0x45F, -80, 0),
    (0x460, 0x481, None, None),
    (0x48A, 0x4BF, None, None),
    (0x4C0, 0x4C0, 0, 15),
    (0x4C1, 0x4CE, None, None),
    (0x4CF, 0x4CF, -15, 0),
    (0x4D0, 0x52F, None, None),
    (0x531, 0x556, 0, 48),
    (0x561, 0x586, -48, 0),
    (0x10A0, 0x10C5, 0, 7264),
    (0x10C7, 0x10C7, 0, 7264),
    (0x10CD, 0x10CD, 0, 7264),
    (0x10D0, 0x10FA, 3008, 0),
    (0x10FD, 0x10FF, 3008, 0),
    (0x13A0, 0x13EF, 0, 38864),
    (0x13F0, 0x13F5, 0, 8),
    (0x13F8, 0x13FD, -8, 0),
    (0x1C80, 0x1C80, -6254, 0),
    (0x1C81, 0x1C81, -6253, 0),
    (0x1C82, 0x1C82, -6244, 0),
    (0x1C83, 0x1C84, -6242, 0),
    (0x1C85, 0x1C85, -6243, 0),
    (0x1C86, 0x1C86, -6236, 0),
    (0x1C87, 0x1C87, -6181, 0),
    (0x1C88, 0x1C88, 35266, 0),
    (0x1C89, 0x1C8A, None, None),
    (0x1C90, 0x1CBA, 0, -3008),
    (0x1CBD, 0x1CBF, 0, -3008),
    (0x1D79, 0x1D79, 35332, 0),
    (0x1D7D, 0x1D7D, 3814, 0),
    (0x1D8E, 0x1D8E, 35384, 0),
    (0x1E00, 0x1E95, None, None),
    (0x1E9B, 0x1E9B, -59, 0),
    (0x1E9E, 0x1E9E, 0, -7615),
    (0x1EA0, 0x1EFF, None, None),
    (0x1F00, 0x1F07, 8, 0),
    (0x1F08, 0x1F0F, 0, -8),
    (0x1F10, 0x1F15, 8, 0),
    (0x1F18, 0x1F1D, 0, -8),
    (0x1F20, 0x1F27, 8, 0),
    (0x1F28, 0x1F2F, 0, -8),
    (0x1F30, 0x1F37, 8, 0),
    (0x1F38, 0x1F3F, 0, -8),
    (0x1F40, 0x1F45, 8, 0),
    (0x1F48, 0x1F4D, 0, -8),
    (0x1F51, 0x1F51, 8, 0),
    (0x1F53, 0x1F53, 8, 0),
    (0x1F55, 0x1F55, 8, 0),
    (0x1F57, 0x1F57, 8, 0),
    (0x1F59, 0x1F59, 0, -8),
    (0x1F5B, 0x1F5B, 0, -8),
    (0x1F5D, 0x1F5D, 0, -8),
    (0x1F5F, 0x1F5F, 0, -8),
    (0x1F60, 0x1F67, 8, 0),
    (0x1F68, 0x1F6F, 0, -8),
    (0x1F70, 0x1F71, 74, 0),
    (0x1F72, 0x1F75, 86, 0),
    (0x1F76, 0x1F77, 100, 0),
    (0x1F78, 0x1F79, 128, 0),
    (0x1F7A, 0x1F7B, 112, 0),
    (0x1F7C, 0x1F7D, 126, 0),
    (0x1F80, 0x1F87, 8, 0),
    (0x1F88, 0x1F8F, 0, -8),
    (0x1F90, 0x1F97, 8, 0),
    (0x1F98, 0x1F9F, 0, -8),
    (0x1FA0, 0x1FA7, 8, 0),
    (0x1FA8, 0x1FAF, 0, -8),
    (0x1FB0, 0x1FB1, 8, 0),
    (0x1FB3, 0x1FB3, 9, 0),
    (0x1FB8, 0x1FB9, 0, -8),
    (0x1FBA, 0x1FBB, 0, -74),
    (0x1FBC, 0x1FBC, 0, -9),
    (0x1FBE, 0x1FBE, -7205, 0),
    (0x1FC3, 0x1FC3, 9, 0),
    (0x1FC8, 0x1FCB, 0, -86),
    (0x1FCC, 0x1FCC, 0, -9),
    (0x1FD0, 0x1FD1, 8, 0),
    (0x1FD8, 0x1FD9, 0, -8),
    (0x1FDA, 0x1FDB, 0, -100),
    (0x1FE0, 0x1FE1, 8, 0),
    (0x1FE5, 0x1FE5, 7, 0),
    (0x1FE8, 0x1FE9, 0, -8),
    (0x1FEA, 0x1FEB, 0, -112),
    (0x1FEC, 0x1FEC, 0, -7),
    (0x1FF3, 0x1FF3, 9, 0),
    (0x1FF8, 0x1FF9, 0, -128),
    (0x1FFA, 0x1FFB, 0, -126),
    (0x1FFC, 0x1FFC, 0, -9),
    (0x2126, 0x2126, 0, -7517),
    (0x212A, 0x212A, 0, -8383),
    (0x212B, 0x212B, 0, -8262),
    (0x2132, 0x2132, 0, 28),
    (0x214E, 0x214E, -28, 0),
    (0x2160, 0x216F, 0, 16),
    (0x2170, 0x217F, -16, 0),
    (0x2183, 0x2184, None, None),
    (0x24B6, 0x24CF, 0, 26),
    (0x24D0, 0x24E9, -26, 0),
    (0x2C00, 0x2C2F, 0, 48),
    (0x2C30, 0x2C5F, -48, 0),
    (0x2C60, 0x2C61, None, None),
    (0x2C62, 0x2C62, 0, -10743),
    (0x2C63, 0x2C63, 0, -3814),
    (0x2C64, 0x2C64, 0, -10727),
    (0x2C65, 0x2C65, -10795, 0),
    (0x2C66, 0x2C66, -10792, 0),
    (0x2C67, 0x2C6C, None, None),
    (0x2C6D, 0x2C6D, 0, -10780),
    (0x2C6E, 0x2C6E, 0, -10749),
    (0x2C6F, 0x2C6F, 0, -10783),
    (0x2C70, 0x2C70, 0, -10782),
    (0x2C72, 0x2C73, None, None),
    (0x2C75, 0x2C76, None, None),
    (0x2C7E, 0x2C7F, 0, -10815),
    (0x2C80, 0x2CE3, None, None),
    (0x2CEB, 0x2CEE, None, None),
    (0x2CF2, 0x2CF3, None, None),
    (0x2D00, 0x2D25, -7264, 0),
    (0x2D27, 0x2D27, -7264, 0),
    (0x2D2D, 0x2D2D, -7264, 0),
    (0xA640, 0xA66D, None, None),
    (0xA680, 0xA69B, None, None),
    (0xA722, 0xA72F, None, None),
    (0xA732, 0xA76F, None, None),
    (0xA779, 0xA77C, None, None),
    (0xA77D, 0xA77D, 0, -35332),
    (0xA77E, 0xA787, None, None),
    (0xA78B, 0xA78C, None, None),
    (0xA78D, 0xA78D, 0, -42280),
    (0xA790, 0xA793, None, None),
    (0xA794, 0xA794, 48, 0),
    (0xA796, 0xA7A9, None, None),
    (0xA7AA, 0xA7AA, 0, -42308),
    (0xA7AB, 0xA7AB, 0, -42319),
    (0xA7AC, 0xA7AC, 0, -42315),
    (0xA7AD, 0xA7AD, 0, -42305),
    (0xA7AE, 0xA7AE, 0, -42308),
    (0xA7B0, 0xA7B0, 0, -42258),
    (0xA7B1, 0xA7B1, 0, -42282),
    (0xA7B2, 0xA7B2, 0, -42261),
    (0xA7B3, 0xA7B3, 0, 928),
    (0xA7B4, 0xA7C3, None, None),
    (0xA7C4, 0xA7C4, 0, -48),
    (0xA7C5, 0xA7C5, 0, -42307),
    (0xA7C6, 0xA7C6, 0, -35384),
    (0xA7C7, 0xA7CA, None, None),
    (0xA7CB, 0xA7CB, 0, -42343),
    (0xA7CC, 0xA7DB, None, None),
    (0xA7DC, 0xA7DC, 0, -42561),
    (0xA7F5, 0xA7F6, None, None),
    (0xAB53, 0xAB53, -928, 0),
    (0xAB70, 0xABBF, -38864, 0),
    (0xFF21, 0xFF3A, 0, 32),
    (0xFF41, 0xFF5A, -32, 0),
    (0x10400, 0x10427, 0, 40),
    (0x10428, 0x1044F, -40, 0),
    (0x104B0, 0x104D3, 0, 40),
    (0x104D8, 0x104FB, -40, 0),
    (0x10570, 0x1057A, 0, 39),
    (0x1057C, 0x1058A, 0, 39),
    (0x1058C, 0x10592, 0, 39),
    (0x10594, 0x10595, 0, 39),
    (0x10597, 0x105A1, -39, 0),
    (0x105A3, 0x105B1, -39, 0),
    (0x105B3, 0x105B9, -39, 0),
    (0x105BB, 0x105BC, -39, 0),
    (0x10C80, 0x10CB2, 0, 64),
    (0x10CC0, 0x10CF2, -64, 0),
    (0x10D50, 0x10D65, 0, 32),
    (0x10D70, 0x10D85, -32, 0),
    (0x118A0, 0x118BF, 0, 32),
    (0x118C0, 0x118DF, -32, 0),
    (0x16E40, 0x16E5F, 0, 32),
    (0x16E60, 0x16E7F, -32, 0),
    (0x16EA0, 0x16EB8, 0, 27),
    (0x16EBB, 0x16ED3, -27, 0),
    (0x1E900, 0x1E921, 0, 34),
    (0x1E922, 0x1E943, -34, 0),
)
GO_CASE_ORBIT = {
    0x4B: 0x6B,
    0x53: 0x73,
    0x6B: 0x212A,
    0x73: 0x17F,
    0xB5: 0x39C,
    0xC5: 0xE5,
    0xDF: 0x1E9E,
    0xE5: 0x212B,
    0x130: 0x130,
    0x131: 0x131,
    0x17F: 0x53,
    0x1C4: 0x1C5,
    0x1C5: 0x1C6,
    0x1C6: 0x1C4,
    0x1C7: 0x1C8,
    0x1C8: 0x1C9,
    0x1C9: 0x1C7,
    0x1CA: 0x1CB,
    0x1CB: 0x1CC,
    0x1CC: 0x1CA,
    0x1F1: 0x1F2,
    0x1F2: 0x1F3,
    0x1F3: 0x1F1,
    0x345: 0x399,
    0x390: 0x1FD3,
    0x392: 0x3B2,
    0x395: 0x3B5,
    0x398: 0x3B8,
    0x399: 0x3B9,
    0x39A: 0x3BA,
    0x39C: 0x3BC,
    0x3A0: 0x3C0,
    0x3A1: 0x3C1,
    0x3A3: 0x3C2,
    0x3A6: 0x3C6,
    0x3A9: 0x3C9,
    0x3B0: 0x1FE3,
    0x3B2: 0x3D0,
    0x3B5: 0x3F5,
    0x3B8: 0x3D1,
    0x3B9: 0x1FBE,
    0x3BA: 0x3F0,
    0x3BC: 0xB5,
    0x3C0: 0x3D6,
    0x3C1: 0x3F1,
    0x3C2: 0x3C3,
    0x3C3: 0x3A3,
    0x3C6: 0x3D5,
    0x3C9: 0x2126,
    0x3D0: 0x392,
    0x3D1: 0x3F4,
    0x3D5: 0x3A6,
    0x3D6: 0x3A0,
    0x3F0: 0x39A,
    0x3F1: 0x3A1,
    0x3F4: 0x398,
    0x3F5: 0x395,
    0x412: 0x432,
    0x414: 0x434,
    0x41E: 0x43E,
    0x421: 0x441,
    0x422: 0x442,
    0x42A: 0x44A,
    0x432: 0x1C80,
    0x434: 0x1C81,
    0x43E: 0x1C82,
    0x441: 0x1C83,
    0x442: 0x1C84,
    0x44A: 0x1C86,
    0x462: 0x463,
    0x463: 0x1C87,
    0x1C80: 0x412,
    0x1C81: 0x414,
    0x1C82: 0x41E,
    0x1C83: 0x421,
    0x1C84: 0x1C85,
    0x1C85: 0x422,
    0x1C86: 0x42A,
    0x1C87: 0x462,
    0x1C88: 0xA64A,
    0x1E60: 0x1E61,
    0x1E61: 0x1E9B,
    0x1E9B: 0x1E60,
    0x1E9E: 0xDF,
    0x1FBE: 0x345,
    0x1FD3: 0x390,
    0x1FE3: 0x3B0,
    0x2126: 0x3A9,
    0x212A: 0x4B,
    0x212B: 0xC5,
    0xA64A: 0xA64B,
    0xA64B: 0x1C88,
    0xFB05: 0xFB06,
    0xFB06: 0xFB05,
}
GO_LETTER_STARTS = tuple(x[0] for x in GO_LETTER_RANGES)
GO_CASE_STARTS = tuple(x[0] for x in GO_CASE_RANGES)


def go_is_letter(rune):
    index = bisect.bisect_right(GO_LETTER_STARTS, rune)-1
    if index < 0:
        return False
    lo, hi, stride = GO_LETTER_RANGES[index]
    return rune <= hi and (rune-lo) % stride == 0


def go_simple_fold(rune):
    if rune in GO_CASE_ORBIT:
        return GO_CASE_ORBIT[rune]
    index = bisect.bisect_right(GO_CASE_STARTS, rune)-1
    if index < 0:
        return rune
    lo, hi, upper, lower = GO_CASE_RANGES[index]
    if rune > hi:
        return rune
    if upper is None:  # Go UpperLower alternating range, starts upper case.
        candidate = lo+(((rune-lo) & ~1) | 1)
        return candidate if candidate != rune else lo+((rune-lo) & ~1)
    candidate = rune+lower
    return candidate if candidate != rune else rune+upper


def go_path_fold(path):
    # x/mod v0.41.0 zip.strToFold: minimum SimpleFold orbit representative,
    # with ASCII A-Z mapped to a-z to preserve its ASCII fast-path equality.
    result = []
    for char in path:
        rune = ord(char)
        for _ in range(8):
            before = rune
            rune = go_simple_fold(rune)
            if rune <= before:
                break
        else:
            fail('PinnedUnicodeTableInvalid')
        if 65 <= rune <= 90:
            rune += 32
        result.append(chr(rune))
    return ''.join(result)


# Go h1 and module ZIP rules are ported from pinned golang.org/x/mod v0.41.0:
# sumdb/dirhash/hash.go Hash1; module/module.go CheckPath/CheckFilePath;
# zip/zip.go checkZip/collisionChecker. Stronger stand limits are explicit.
WINDOWS_BAD = frozenset(['con', 'prn', 'aux', 'nul']+
                       ['com'+str(i) for i in range(1, 10)]+['lpt'+str(i) for i in range(1, 10)])
FILE_PUNCTUATION = '!#$%&()+,-.=@[]^_{}~ '


def module_file_path(path):
    relative_path(path)
    for part in path.split('/'):
        if set(part) == {'.'} or part.endswith('.') or go_path_fold(part.split('.')[0]) in WINDOWS_BAD:
            fail('ModulePath')
        for char in part:
            if ord(char) < 128:
                valid = char.isascii() and (char.isalnum() or char in FILE_PUNCTUATION)
            else:
                valid = go_is_letter(ord(char))
            if not valid:
                fail('ModulePath')
    return path


def check_module(path, version):
    relative_path(path)
    first = path.split('/')[0]
    if '.' not in first or first.startswith('-') or not re.fullmatch(r'[a-z0-9.-]+', first):
        fail('ModulePath')
    for part in path.split('/'):
        if part.startswith('.') or part.endswith('.') or not re.fullmatch(r'[A-Za-z0-9._~-]+', part) or part.split('.')[0].lower() in WINDOWS_BAD or re.search(r'~[0-9]+$', part.split('.')[0]):
            fail('ModulePath')
    match = re.fullmatch(MODULE_VERSION, version)
    if not match:
        fail('ModuleVersion')
    for item in (match.group(4) or '').lstrip('-').split('.'):
        if item.isdigit() and len(item) > 1 and item.startswith('0'):
            fail('ModuleVersion')
    major = int(match.group(1))
    suffix = re.search(r'/v([0-9.]+)$', path)
    if path.startswith('gopkg.in/'):
        suffix = re.search(r'\.v([0-9]+)(?:-unstable)?$', path)
        if suffix is None:
            fail('ModuleVersion')
    if suffix:
        number = suffix.group(1)
        if not number.isdigit() or (len(number) > 1 and number.startswith('0')) or (not path.startswith('gopkg.in/') and int(number) < 2) or int(number) != major:
            fail('ModuleVersion')
    elif major >= 2 and not version.endswith('+incompatible'):
        fail('ModuleVersion')


def escape_module(value):
    return ''.join('!'+char.lower() if 'A' <= char <= 'Z' else char for char in value)


def h1_files(file_hashes):
    digest = hashlib.sha256()
    for name in sorted(file_hashes):
        if '\n' in name or '\r' in name or not re.fullmatch(HASH, file_hashes[name]):
            fail('H1Input')
        digest.update((file_hashes[name]+'  '+name+'\n').encode('utf-8'))
    return 'h1:'+base64.b64encode(digest.digest()).decode('ascii')


def mod_h1(raw):
    return h1_files({'go.mod': hashlib.sha256(raw).hexdigest()})


def parse_go_sum(raw):
    values = {}
    try:
        text = raw.decode('utf-8')
    except UnicodeError as exc:
        raise Failure('GoSumInvalid') from exc
    for line in text.splitlines():
        if not line:
            continue
        parts = line.split()
        if len(parts) != 3 or not re.fullmatch(H1, parts[2]):
            fail('GoSumInvalid')
        module, version, digest = parts
        actual_version = version.removesuffix('/go.mod')
        check_module(module, actual_version)
        key = (module, version)
        if key in values and values[key] != digest:
            fail('GoSumConflict')
        values[key] = digest
    return values


def zip_h1(fd, module, version, budget, expansion_budget=None):
    check_module(module, version)
    before = check_regular(fd, LIMITS['zip'])
    prefix = module+'@'+version+'/'
    file_hashes, collisions, total = {}, {}, 0
    duplicate_dirs = set()
    _zip_central_bound(fd, before.st_size)
    with os.fdopen(os.dup(fd), 'rb') as stream, zipfile.ZipFile(stream) as archive:
        infos = archive.infolist()
        if len(infos) > LIMITS['zip_entries']:
            fail('ExpansionLimit')
        if expansion_budget is not None:
            expansion_budget[0] += len(infos)
            if expansion_budget[0] > LIMITS['all_zip_entries']:
                fail('ExpansionLimit')
        for entry in infos:
            budget.check()
            if not entry.flag_bits & 0x800:
                try:
                    entry.filename = entry.filename.encode('cp437').decode('utf-8')
                except UnicodeError as exc:
                    raise Failure('ModuleZipEncoding') from exc
                entry.orig_filename = entry.filename
            if '\0' in entry.orig_filename:
                fail('ModuleZipInvalid')
            if not entry.filename.startswith(prefix) or entry.flag_bits & 1 or entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                fail('ModuleZipInvalid')
            relative = entry.filename[len(prefix):]
            if not relative:
                if not entry.is_dir() or entry.file_size != 0:
                    fail('ModuleZipInvalid')
                if entry.filename in file_hashes:
                    fail('ModuleZipDuplicate')
                file_hashes[entry.filename] = _zip_entry_hash(fd, entry, budget)
                continue
            is_dir = entry.is_dir()
            name = module_file_path(relative[:-1] if is_dir else relative)
            raw_mode = (entry.external_attr >> 16) & 0xffff
            typ = stat.S_IFMT(raw_mode)
            if typ and typ != (stat.S_IFDIR if is_dir else stat.S_IFREG):
                fail('ModuleZipIrregular')
            parts = name.split('/')
            for depth in range(1, len(parts)+1):
                parent = '/'.join(parts[:depth])
                directory = is_dir or depth != len(parts)
                fold = go_path_fold(parent)
                old = collisions.get(fold)
                if old is not None and old != (parent, directory):
                    fail('ModuleZipCollision')
                if depth == len(parts) and not is_dir and old is not None:
                    fail('ModuleZipDuplicate')
                collisions[fold] = (parent, directory)
            if is_dir:
                if name in duplicate_dirs:
                    fail('ModuleZipDuplicate')
                duplicate_dirs.add(name)
                if entry.file_size != 0:
                    fail('ModuleZipInvalid')
                file_hashes[entry.filename] = _zip_entry_hash(fd, entry, budget)
                continue
            if go_path_fold(name.rsplit('/', 1)[-1]) == 'go.mod' and name != 'go.mod':
                fail('ModuleZipGoMod')
            if name in ('go.mod', 'LICENSE') and entry.file_size > 16*MIB:
                fail('ExpansionLimit')
            if entry.file_size > LIMITS['source_file']:
                fail('ExpansionLimit')
            total += entry.file_size
            if total > min(LIMITS['module_expand'], 500*MIB):
                fail('ExpansionLimit')
            if expansion_budget is not None:
                expansion_budget[1] += entry.file_size
                if expansion_budget[1] > LIMITS['lane_expand']:
                    fail('ExpansionLimit')
            file_hashes[prefix+name] = _zip_entry_hash(fd, entry, budget)
    if stable_stat(before) != stable_stat(check_regular(fd, LIMITS['zip'])):
        fail('InputMutation', 'seed', True)
    return h1_files(file_hashes), file_hashes


def walk_regular(cap, budget, max_files, max_bytes, max_file, prefix=''):
    result, total = [], 0
    for name in cap.names():
        budget.check()
        relative_path(name)
        st = os.stat(name, dir_fd=cap.fd, follow_symlinks=False)
        path = prefix+name
        if stat.S_ISDIR(st.st_mode):
            with cap.directory(name) as child:
                nested = walk_regular(child, budget, max_files-len(result), max_bytes-total, max_file, path+'/')
            result.extend(nested)
            total += sum(item['size'] for item in nested)
        else:
            fd = cap.file(name)
            try:
                digest, actual = hash_fd(fd, max_file, budget)
            finally:
                os.close(fd)
            total += actual.st_size
            result.append({'path': path, 'size': actual.st_size, 'sha256': digest,
                           'mode': '100755' if actual.st_mode & 0o111 else '100644'})
        if len(result) > max_files or total > max_bytes:
            fail('ManifestLimit')
    return result


class Seed:
    def __init__(self, spec, sums, budget):
        self.pinned, self.manifest_fd, self.root = {}, None, None
        try:
            self.root = Directory.open_absolute(spec['root'])
            self.budget, self.sums, self.pinned, self.modules = budget, sums, {}, {}
            self.manifest_sha256 = spec['manifest_sha256']
            self.manifest_fd = None
            self.manifest_fd = open_absolute_file(spec['manifest_path'], LIMITS['seed_json'], budget)
            raw = read_fd(self.manifest_fd, LIMITS['seed_json'], budget)
            if hashlib.sha256(raw).hexdigest() != spec['manifest_sha256']:
                fail('SeedManifestMismatch')
            self.manifest = validate('seed', decode_json(raw, LIMITS['seed_json']))
            total, expansion, object_count = 0, [0, 0], 0
            expected_names = set()
            for module in self.manifest['modules']:
                key = (module['path'], module['version'])
                check_module(*key)
                if key in self.modules:
                    fail('DuplicateSeedModule')
                kinds, verified = set(), {'path': key[0], 'version': key[1], 'files': module['files'], 'zip_h1': None}
                for entry in module['files']:
                    kind = entry['kind']
                    if kind in kinds:
                        fail('DuplicateSeedObject')
                    kinds.add(kind)
                    expected = 'cache/download/'+escape_module(key[0])+'/@v/'+escape_module(key[1])+'.'+kind
                    if entry['path'] != expected or expected in expected_names:
                        fail('SeedPathMismatch')
                    expected_names.add(expected)
                    object_count += 1
                    if object_count > LIMITS['seed_objects']:
                        fail('SeedLimit')
                    limit = LIMITS[kind]
                    file_fd = self.root.file(entry['path'])
                    self.pinned[entry['path']] = (file_fd, entry)
                    digest, st = hash_fd(file_fd, limit, budget)
                    if digest != entry['sha256'] or st.st_size != entry['size']:
                        fail('SeedMismatch')
                    total += st.st_size
                    if total > LIMITS['seed_total']:
                        fail('SeedLimit')
                    if kind == 'mod':
                        h1 = mod_h1(read_fd(file_fd, LIMITS['mod'], budget))
                        if sums.get((key[0], key[1]+'/go.mod')) != h1:
                            fail('GoSumMismatch')
                        verified['mod_h1'] = h1
                    elif kind == 'zip':
                        h1, _ = zip_h1(file_fd, *key, budget, expansion)
                        if sums.get(key) != h1:
                            fail('GoSumMismatch')
                        verified['zip_h1'] = h1
                    elif kind == 'info':
                        info = decode_json(read_fd(file_fd, LIMITS['info'], budget), LIMITS['info'])
                        if type(info) is not dict or set(info) != {'Version', 'Time'} or info['Version'] != key[1] or type(info['Time']) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z', info['Time']):
                            fail('SeedInfoMismatch')
                        try:
                            datetime.datetime.fromisoformat(info['Time'][:-1]+'+00:00')
                        except ValueError as exc:
                            raise Failure('SeedInfoMismatch') from exc
                    # Caller ziphash bytes are verified copied evidence only. They
                    # never authenticate ZIP contents; destinations get recomputed h1.
                if 'mod' not in kinds:
                    fail('OfflineDependencyMissing')
                self.modules[key] = verified
            actual = {x['path'] for x in walk_regular(self.root, budget, LIMITS['seed_objects'], LIMITS['seed_total'], LIMITS['zip'])}
            if actual != expected_names:
                fail('UnexpectedSeedFiles')
            self.digest = domain_hash('seed', self.manifest)
        except BaseException:
            self.close()
            raise

    def recheck(self):
        if hashlib.sha256(read_fd(self.manifest_fd, LIMITS['seed_json'], self.budget)).hexdigest() != self.manifest_sha256:
            fail('InputMutation', 'seed_manifest', True)
        for _, (fd, entry) in self.pinned.items():
            digest, st = hash_fd(fd, LIMITS[entry['kind']], self.budget)
            if digest != entry['sha256'] or st.st_size != entry['size']:
                fail('InputMutation', 'seed', True)
        return self.digest

    def copy_to(self, destination):
        if destination.names():
            fail('CacheNotEmpty')
        for path, (original, entry) in sorted(self.pinned.items()):
            fd = destination.file(path, create=True)
            try:
                before = check_regular(original, LIMITS[entry['kind']])
                os.lseek(original, 0, os.SEEK_SET)
                size = 0
                while True:
                    self.budget.check()
                    chunk = os.read(original, min(MIB, entry['size']+1-size))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > entry['size']:
                        fail('SeedMutation', 'seed', True)
                    _write_all(fd, chunk, self.budget)
                digest, st = hash_fd(fd, LIMITS[entry['kind']], self.budget)
                if digest != entry['sha256'] or size != entry['size'] or st.st_size != size or stable_stat(before) != stable_stat(os.fstat(original)):
                    fail('SeedMutation', 'seed', True)
            finally:
                os.close(fd)
        for module in self.modules.values():
            if module['zip_h1'] is None:
                continue
            path = 'cache/download/'+escape_module(module['path'])+'/@v/'+escape_module(module['version'])+'.ziphash'
            try:
                old = destination.file(path)
            except FileNotFoundError:
                old = None
            if old is not None:
                os.close(old)
                parent, name = path.rsplit('/', 1)
                with destination.directory(parent) as cap:
                    os.unlink(name, dir_fd=cap.fd)
            fd = destination.file(path, create=True)
            try:
                expected = module['zip_h1'].encode()
                _write_all(fd, expected, self.budget)
                actual, st = hash_fd(fd, LIMITS['ziphash'], self.budget)
                if actual != hashlib.sha256(expected).hexdigest() or st.st_size != len(expected):
                    fail('SeedDestinationMismatch')
            finally:
                os.close(fd)
        self.recheck()

    def close(self):
        for fd, _ in self.pinned.values():
            os.close(fd)
        self.pinned.clear()
        if self.manifest_fd is not None:
            os.close(self.manifest_fd)
            self.manifest_fd = None
        if self.root is not None:
            self.root.close()


class Fingerprint:
    def __init__(self, root, manifest_spec, budget, complete=True):
        self.root, self.budget, self.complete = root, budget, complete
        self.manifest_sha256 = manifest_spec['manifest_sha256']
        self.manifest_fd = None
        try:
            self.manifest_fd = open_absolute_file(manifest_spec['manifest_path'], LIMITS['manifest_json'], budget)
            raw = read_fd(self.manifest_fd, LIMITS['manifest_json'], budget)
            if hashlib.sha256(raw).hexdigest() != manifest_spec['manifest_sha256']:
                fail('FingerprintManifestMismatch')
            self.manifest = validate('fingerprint', decode_json(raw, LIMITS['manifest_json']))
            paths = [x['path'] for x in self.manifest['files']]
            if paths != sorted(set(paths)):
                fail('FingerprintMembership')
            self.initial = self.verify()
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.manifest_fd is not None:
            os.close(self.manifest_fd)
            self.manifest_fd = None

    def verify(self):
        self.root.verify_name()
        if hashlib.sha256(read_fd(self.manifest_fd, LIMITS['manifest_json'], self.budget)).hexdigest() != self.manifest_sha256:
            fail('InputMutation', 'fingerprint_manifest', True)
        if self.complete:
            actual = walk_regular(self.root, self.budget, LIMITS['tool_entries'], LIMITS['tool_total'], LIMITS['tool_file'])
        else:
            actual, total = [], 0
            for item in self.manifest['files']:
                fd = self.root.file(item['path'])
                try:
                    digest, st = hash_fd(fd, LIMITS['tool_file'], self.budget)
                finally:
                    os.close(fd)
                total += st.st_size
                if total > LIMITS['tool_total']:
                    fail('ToolchainLimit')
                actual.append({'path': item['path'], 'size': st.st_size, 'sha256': digest,
                               'mode': '100755' if st.st_mode & 0o111 else '100644'})
        if sorted(actual, key=lambda x: x['path']) != self.manifest['files']:
            fail('InputMutation', 'fingerprint', True)
        return domain_hash('toolchain', self.manifest)


def _contains(root, path):
    return path == root or path.startswith(root.rstrip('/')+'/')


def _public_native_path(path):
    path = os.path.realpath(absolute_path(path))
    if not any(_contains(prefix, path) for prefix in ('/usr', '/lib', '/lib64', '/opt')):
        fail('NativePathNotPublic')
    return path


def _command_bytes(supervisor, executable, argv, cwd, env, budget, output_class='ordinary'):
    budget.check()
    seconds = LIMITS['graph_seconds'] if output_class == 'graph' else LIMITS['command_seconds']
    result = supervisor.run_owned(tuple([executable.path]+list(argv)), executable, cwd, env,
                                   min(seconds, max(0, budget.end-time.monotonic())), output_class)
    require_command(result)
    fd = supervisor.store.root.file(result.stdout)
    try:
        raw = read_fd(fd, LIMITS['graph_stdout'] if output_class == 'graph' else LIMITS['stdout'], budget)
    finally:
        os.close(fd)
    return raw, result


def _json_stream(raw, maximum):
    if len(raw) > LIMITS['graph_stdout']:
        fail('OutputLimit')
    try:
        text = raw.decode('utf-8')
    except UnicodeError as exc:
        raise Failure('GraphParse') from exc
    decoder = json.JSONDecoder(object_pairs_hook=_unique_pairs, parse_float=_reject_number,
                               parse_constant=_reject_number)
    offset, values = 0, []
    # Bound lexical depth before decoder recursion even though stdout may be a
    # concatenated sequence rather than one JSON array.
    _lexical_depth(raw)
    while offset < len(text):
        while offset < len(text) and text[offset].isspace():
            offset += 1
        if offset == len(text):
            break
        try:
            value, offset = decoder.raw_decode(text, offset)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise Failure('GraphParse') from exc
        _json_limits(value)
        if type(value) is not dict:
            fail('GraphParse')
        values.append(value)
        if len(values) > maximum:
            fail('ClosureLimit')
    return values


def initialize_lane_telemetry(home, budget):
    # The original isolated XDG_CONFIG_HOME, never the caller's global config.
    # GOTELEMETRY is output-only in this Go version: its mode FILE controls the
    # daemonized telemetry sidecar, which would violate owned group membership.
    if home.names():
        fail('TelemetryHomeNotFresh', 'toolchain')
    with home.directory('go/telemetry', create=True) as directory:
        fd = directory.file('mode', create=True)
        try:
            _write_all(fd, b'off\n', budget)
            os.fsync(fd)
            expected = stable_stat(check_regular(fd, 4, private=True))
        finally:
            os.close(fd)
        os.fsync(directory.fd)
    verify_lane_telemetry(home, expected, budget)
    return expected


def verify_lane_telemetry(home, expected, budget):
    home.verify_name()
    with home.directory('go') as go, go.directory('telemetry') as directory:
        for cap in (home, go, directory):
            st = os.fstat(cap.fd)
            # HOME is the original own private root. Its nested config
            # ancestors retain Directory's trusted-root-or-own owner law;
            # mapped root ownership never permits another user's writes.
            owners = (os.getuid(),) if cap is home else (0, os.getuid())
            if st.st_uid not in owners or stat.S_IMODE(st.st_mode) != 0o700:
                fail('UnsafePrivateDirectory', 'toolchain')
            cap.verify_name()
        fd = directory.file('mode')
        try:
            if read_fd(fd, 4, budget, private=True) != b'off\n' or stable_stat(os.fstat(fd)) != expected:
                fail('TelemetryModeChanged', 'toolchain', True)
            named = os.stat('mode', dir_fd=directory.fd, follow_symlinks=False)
            if stable_stat(named) != expected:
                fail('TelemetryModeChanged', 'toolchain', True)
        finally:
            os.close(fd)


def environment_for_lane(request, lane, goroot, tools_path, epoch):
    # No inheritance. No CORSA_, proxy, credential or compiler-wrapper values.
    env = {'PATH': tools_path, 'HOME': lane['home'].label, 'XDG_CONFIG_HOME': lane['home'].label,
           'TMPDIR': lane['tmp'].label, 'GOPATH': lane['gopath'].label,
           'GOMODCACHE': lane['modules'].label, 'GOCACHE': lane['cache'].label,
           'GOROOT': goroot, 'GOTOOLCHAIN': 'local', 'GOENV': 'off', 'GOWORK': 'off',
           'GO111MODULE': 'on', 'GOFLAGS': '', 'GOPROXY': 'off', 'GOSUMDB': 'off',
           'GOOS': 'linux', 'GOARCH': 'arm64', 'GOARM64': 'v8.0', 'GOEXPERIMENT': '',
           'CGO_ENABLED': '1' if request['flavour'] == 'native-linux-arm64' else '0',
           'SOURCE_DATE_EPOCH': str(epoch), 'LANG': 'C', 'LC_ALL': 'C', 'TZ': 'UTC'}
    if request['native']:
        env.update(CC=request['native']['cc']['path'], CXX=request['native']['cxx']['path'],
                   PKG_CONFIG=request['native']['pkg_config']['path'],
                   CGO_CFLAGS='-O2 -g', CGO_CXXFLAGS='-O2 -g', CGO_CPPFLAGS='', CGO_LDFLAGS='-O2 -g')
    return env


def git_environment(home):
    return {'PATH': '/nonexistent', 'HOME': home, 'XDG_CONFIG_HOME': home,
            'LANG': 'C', 'LC_ALL': 'C', 'GIT_CONFIG_NOSYSTEM': '1',
            'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_SYSTEM': '/dev/null',
            'GIT_NO_REPLACE_OBJECTS': '1', 'GIT_OPTIONAL_LOCKS': '0',
            'GIT_NO_LAZY_FETCH': '1', 'GIT_TERMINAL_PROMPT': '0', 'GIT_PAGER': ''}


def new_lane(root, name, registry):
    cap = root.directory('work/'+name, create=True)
    result = {'root': cap}
    registry.append(result)  # own partial construction BEFORE another fallible open
    for key, path in (('source', 'source'), ('modules', 'modules'), ('cache', 'gocache'),
                      ('home', 'home'), ('tmp', 'tmp'), ('gopath', 'gopath'), ('bin', 'bin')):
        result[key] = cap.directory(path, create=True)
    identities = [v.identity for v in result.values()]
    if len(set(identities)) != len(identities):
        fail('AliasedLaneRoot')
    return result


def check_root_names(lane):
    for cap in lane.values():
        with Directory.open_absolute(cap.label) as actual:
            if actual.identity != cap.identity:
                fail('InputMutation', 'root', True)


def measure_tree(cap, budget, maximum, depth=0):
    if depth > LIMITS['path_depth']:
        fail('WorkLimit', 'working', True)
    total = 0
    for name in cap.names():
        budget.check()
        relative_path(name)
        try:
            st = os.stat(name, dir_fd=cap.fd, follow_symlinks=False)
        except FileNotFoundError:
            continue  # transient tool-owned rename; final inventories recheck
        if stat.S_ISDIR(st.st_mode):
            try:
                with cap.directory(name) as child:
                    total += measure_tree(child, budget, maximum-total, depth+1)
            except FileNotFoundError:
                continue
        elif stat.S_ISREG(st.st_mode):
            total += max(st.st_size, st.st_blocks*512)
        else:
            fail('IrregularWorkFile', 'working', True)
        if total > maximum:
            fail('WorkLimit', 'working', True)
    return total


def check_source(cap, original, budget):
    actual = walk_regular(cap, budget, LIMITS['source_files'], LIMITS['source_total'], LIMITS['source_file'])
    expected = [{k: entry[k] for k in ('path', 'size', 'sha256', 'mode')} for entry in original['files']]
    if sorted(actual, key=lambda x: x['path']) != expected:
        fail('InputMutation', 'source', True)
    return source_digest(original['files'])


def verify_module_tree(cap, module, budget):
    relative = escape_module(module['path'])+'@'+escape_module(module['version'])
    with cap.directory(relative) as tree:
        entries = walk_regular(tree, budget, LIMITS['zip_entries'], LIMITS['module_expand'], LIMITS['source_file'])
    prefix = module['path']+'@'+module['version']+'/'
    actual = h1_files({prefix+x['path']: x['sha256'] for x in entries})
    if module['zip_h1'] is None or actual != module['zip_h1']:
        fail('ModuleTreeMismatch')
    return actual


def verify_seed_destination(seed, cap, budget):
    expected = {path: (entry['sha256'], entry['size'], LIMITS[entry['kind']])
                for path, (_, entry) in seed.pinned.items()}
    # ziphash may not be present in caller input. It is still a newly consumed
    # destination object and must be rechecked in BOTH independently seeded lanes.
    for module in seed.modules.values():
        if module['zip_h1'] is not None:
            path = 'cache/download/'+escape_module(module['path'])+'/@v/'+escape_module(module['version'])+'.ziphash'
            raw = module['zip_h1'].encode()
            expected[path] = (hashlib.sha256(raw).hexdigest(), len(raw), LIMITS['ziphash'])
    for path, (digest, size, limit) in expected.items():
        fd = cap.file(path)
        try:
            actual, st = hash_fd(fd, limit, budget)
        finally:
            os.close(fd)
        if actual != digest or st.st_size != size:
            fail('InputMutation', 'seed_destination', True)
    seed.recheck()
    return seed.digest


def check_native_flags(arguments, source_root, package_root, native_roots):
    """Finite recorded-input flag subset; unknown compiler input mechanisms refuse."""
    if type(arguments) is not list or len(arguments) > LIMITS['array']:
        fail('NativeFlagsInvalid')
    pending = False
    for flag in arguments:
        if type(flag) is not str or not flag or len(flag.encode()) > LIMITS['argv'] or any(ord(x) < 32 or ord(x) == 127 for x in flag):
            fail('NativeFlagsInvalid')
        path = None
        if pending:
            if flag.startswith('-'): fail('NativeFlagsUnsupported')
            path, pending = flag, False
        elif flag in ('-I', '-L', '-isystem', '-iquote'):
            pending = True
            continue
        elif flag.startswith(('-I', '-L')) and len(flag) > 2:
            path = flag[2:]
        elif (flag in ('-pthread', '-static', '-shared', '-fPIC', '-fpic', '-fPIE', '-fpie', '-fno-strict-aliasing', '-fno-common', '-fno-stack-protector', '-Wdeclaration-after-statement') or
              re.fullmatch(r'-(?:O[0-3sg]|g[0-3]?|D[A-Za-z_][A-Za-z0-9_]*(?:=.*)?|U[A-Za-z_][A-Za-z0-9_]*|l:?[-A-Za-z0-9_.+]+|std=[A-Za-z0-9+]+|W(?:all|extra|error|no-[A-Za-z0-9-]+)|m(?:arch|cpu|tune)=[A-Za-z0-9_.+-]+)', flag)):
            continue
        else:
            fail('NativeFlagsUnsupported')
        normalized = os.path.normpath(path if path.startswith('/') else package_root+'/'+path)
        absolute_path(normalized)
        if not _contains(source_root, normalized) and not _contains(package_root, normalized):
            native_public(normalized, native_roots)
    if pending:
        fail('NativeFlagsInvalid')


def graph_and_closure(supervisor, go, lane, env, request, seed, source_manifest, budget, native):
    graph_raw, _ = _command_bytes(supervisor, go, ['list', '-mod=readonly', '-m', '-json', 'all'], lane['source'], env, budget, 'graph')
    graph = _json_stream(graph_raw, LIMITS['modules'])
    selected = {}
    main_path = None
    for item in graph:
        if item.get('Replace') is not None or item.get('Error') is not None or 'Path' not in item:
            fail('UnsupportedModuleGraph')
        if item.get('Main') is True:
            if main_path is not None or item.get('Dir') != lane['source'].label:
                fail('MainModuleBinding')
            main_path = item['Path']
            continue
        key = (item['Path'], item.get('Version'))
        if key not in seed.modules or 'mod_h1' not in seed.modules[key]:
            fail('OfflineDependencyMissing')
        if item.get('GoMod') != lane['modules'].label+'/cache/download/'+escape_module(key[0])+'/@v/'+escape_module(key[1])+'.mod':
            fail('ModulePathBinding')
        selected[key] = {'path': key[0], 'version': key[1], 'status': 'graph_only',
                         'mod_h1': seed.modules[key]['mod_h1'], 'zip_h1': None}
    if main_path is None:
        fail('MainModuleBinding')
    packages_raw, _ = _command_bytes(supervisor, go, ['list', '-mod=readonly', '-deps', '-json']+
                                     [TARGETS[x] for x in request['targets']], lane['source'], env, budget, 'graph')
    packages = _json_stream(packages_raw, LIMITS['packages'])
    files, used, imports = {}, set(), set()
    source = {x['path']: x for x in source_manifest['files']}
    file_fields = ('GoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles', 'HFiles',
                   'FFiles', 'SFiles', 'SwigFiles', 'SwigCXXFiles', 'SysoFiles', 'EmbedFiles')
    for pkg in packages:
        budget.check()
        if pkg.get('Error') is not None or pkg.get('DepsErrors') or type(pkg.get('Dir')) is not str or pkg.get('ImportPath') in imports:
            fail('PackageClosureInvalid')
        imports.add(pkg.get('ImportPath'))
        if any(pkg.get(k) for k in ('FFiles', 'SwigFiles', 'SwigCXXFiles')):
            fail('NativeInputsUnsupported')
        directory = pkg['Dir']
        module = pkg.get('Module')
        if module and module.get('Replace') is not None:
            fail('UnsupportedModuleGraph')
        flag_source_root = lane['source'].label
        if pkg.get('Standard') is True:
            # Complete GOROOT covers source bytes, not arbitrary Cgo operands.
            if not _contains(env['GOROOT']+'/src', directory):
                fail('OutsideClosure')
            flag_source_root, package_root = env['GOROOT'], directory
        else:
            if type(module) is not dict:
                fail('OutsideClosure')
            own = module.get('Main') is True
            if own:
                if module.get('Path') != main_path or not _contains(lane['source'].label, directory):
                    fail('OutsideClosure')
                base = lane['source'].label
                cap = lane['source']
                prefix = ''
            else:
                key = (module.get('Path'), module.get('Version'))
                if key not in selected or seed.modules[key]['zip_h1'] is None:
                    fail('OfflineDependencyMissing')
                used.add(key)
                relative = escape_module(key[0])+'@'+escape_module(key[1])
                base = lane['modules'].label+'/'+relative
                if not _contains(base, directory) or module.get('Dir') != base:
                    fail('OutsideClosure')
                cap = lane['modules']
                prefix = relative+'/'
            local_directory = os.path.relpath(directory, base)
            for field in file_fields:
                items = pkg.get(field, [])
                if type(items) is not list or len(items) > LIMITS['closure_files']:
                    fail('ClosureLimit')
                for filename in items:
                    relative_path(filename)
                    local = (local_directory+'/' if local_directory != '.' else '')+filename
                    relative_path(local)
                    fd = cap.file(prefix+local)
                    try:
                        digest, st = hash_fd(fd, LIMITS['source_file'], budget)
                    finally:
                        os.close(fd)
                    if own and (local not in source or source[local]['sha256'] != digest):
                        fail('MissingSourceClosure')
                    label = ('source/' if own else 'module/'+module['Path']+'@'+module['Version']+'/')+local
                    files[label] = {'path': label, 'sha256': digest, 'size': st.st_size}
                    if len(files) > LIMITS['closure_files']:
                        fail('ClosureLimit')
            package_root = base if not own else directory
        for field in ('CgoCFLAGS', 'CgoCPPFLAGS', 'CgoCXXFLAGS', 'CgoFFLAGS', 'CgoLDFLAGS'):
            flags = pkg.get(field, [])
            if flags and native is None:
                fail('NativeCompilerMissing')
            check_native_flags(flags, flag_source_root, package_root,
                               request['native']['roots'] if request['native'] else [])
        packages = pkg.get('CgoPkgConfig', [])
        if type(packages) is not list or len(packages) > LIMITS['modules']:
            fail('NativeFlagsInvalid')
        if packages:
            if native is None or any(type(x) is not str or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]*', x) is None for x in packages):
                fail('NativeFlagsUnsupported')
            raw, _ = _command_bytes(supervisor, native.tools['pkg_config'],
                                    ['--cflags', '--libs', '--']+packages, lane['source'], env, budget)
            try:
                flags = shlex.split(raw.decode('utf-8'), posix=True)
            except (UnicodeError, ValueError) as exc:
                raise Failure('NativeFlagsInvalid') from exc
            check_native_flags(flags, flag_source_root, package_root, request['native']['roots'])
    for key in sorted(used):
        digest = verify_module_tree(lane['modules'], seed.modules[key], budget)
        selected[key].update(status='used_verified', zip_h1=digest)
    _command_bytes(supervisor, go, ['mod', 'verify'], lane['source'], env, budget)
    normalized_graph = {'schema_version': 2, 'main': main_path, 'modules': [selected[x] for x in sorted(selected)]}
    closure = {'schema_version': 2, 'packages': sorted(imports), 'files': [files[x] for x in sorted(files)]}
    if len(canonical(closure)) > LIMITS['manifest_json']:
        fail('ClosureLimit')
    return normalized_graph, closure, used


DEFS.update({
    'fd_identity': {'dev': I(2**64-1), 'ino': I(2**64-1, 1), 'uid': I(2**32-1), 'mode': I(0o7777)},
    'native_regular': {'public_path': ABS, 'sha256': D, 'size': I(LIMITS['tool_file']), 'identity': R('fd_identity')},
    'native_link_step': {'path': ABS, 'text_sha256': D, 'text_size': I(LIMITS['path'], 1), 'identity': R('fd_identity')},
    'native_link': {'steps': A(R('native_link_step'), 32, 1), 'resolved_regular': R('native_regular')},
    'native_file': {'kind': E('regular', 'symlink'), 'path': ABS, 'regular': N(R('native_regular')), 'link': N(R('native_link'))},
    'native_manifest': {'schema_version': E(2), 'roots': A(ABS, 32, 1, True),
                         'files': A(R('native_file'), LIMITS['tool_entries'], 1)},
})


def fd_identity(st):
    return {'dev': st.st_dev, 'ino': st.st_ino, 'uid': st.st_uid,
            'mode': stat.S_IMODE(st.st_mode)}


def _native_record(record):
    regular = record['kind'] == 'regular'
    if regular != (record['regular'] is not None) or regular == (record['link'] is not None):
        fail('NativeFileDiscriminant')
    if regular and record['regular']['public_path'] != record['path']:
        fail('NativeFilePath')
    if not regular and record['link']['steps'][0]['path'] != record['path']:
        fail('NativeFilePath')


SEMANTIC['native_file'] = _native_record
PUBLIC_NATIVE_PREFIXES = ('/usr', '/lib', '/lib64', '/opt')


def native_public(path, roots):
    absolute_path(path)
    if not any(_contains(prefix, path) for prefix in PUBLIC_NATIVE_PREFIXES) or not any(_contains(root, path) for root in roots):
        fail('NativePathEscape')
    return path


class NativeFile:
    def __init__(self, record, roots, budget):
        validate('native_file', record)
        self.record, self.roots, self.budget = record, roots, budget
        self.links, self.target, self.parent = [], None, None
        self.target_name, self.target_before = None, None
        try:
            self._pin()
        except BaseException:
            self.close()
            raise

    def _pin(self):
        import posixpath
        path = native_public(self.record['path'], self.roots)
        seen, actual_steps = set(), []
        for _ in range(33):
            self.budget.check()
            parent_path, name = path.rsplit('/', 1)
            try:
                parent = Directory.open_absolute(parent_path)
            except OSError as exc:
                raise Failure('NativeInputsUnsupported') from exc
            try:
                st = os.stat(name, dir_fd=parent.fd, follow_symlinks=False)
            except BaseException:
                parent.close()
                raise
            if stat.S_ISLNK(st.st_mode):
                try:
                    if len(actual_steps) == 32 or (st.st_dev, st.st_ino) in seen or st.st_uid not in (0, os.getuid()):
                        parent.close()
                        fail('NativeLinkCycleOrLimit')
                    seen.add((st.st_dev, st.st_ino))
                    try:
                        raw = os.readlink(os.fsencode(name), dir_fd=parent.fd)
                    except BaseException:
                        parent.close()
                        raise
                    try:
                        text = raw.decode('utf-8')
                    except UnicodeError as exc:
                        parent.close()
                        raise Failure('NativeLinkEncoding') from exc
                    if not raw or len(raw) > LIMITS['path'] or '\0' in text or any(ord(c) < 32 or ord(c) == 127 for c in text):
                        parent.close()
                        fail('NativeLinkEncoding')
                    if stable_stat(st) != stable_stat(os.stat(name, dir_fd=parent.fd, follow_symlinks=False)):
                        parent.close()
                        fail('InputMutation', 'native_link', True)
                    step = {'path': path, 'text_sha256': hashlib.sha256(raw).hexdigest(),
                            'text_size': len(raw), 'identity': fd_identity(st)}
                    actual_steps.append(step)
                except BaseException:
                    parent.close()
                    raise
                self.links.append((parent, name, raw, stable_stat(st)))
                path = posixpath.normpath(text if text.startswith('/') else parent_path+'/'+text)
                native_public(path, self.roots)
                continue
            if not stat.S_ISREG(st.st_mode):
                parent.close()
                fail('NativeInputsUnsupported')  # directory symlinks/other types
            self.parent, self.target_name = parent, name
            self.target = parent.file(name)
            digest, observed = hash_fd(self.target, LIMITS['tool_file'], self.budget)
            self.target_before = stable_stat(observed)
            regular = {'public_path': path, 'sha256': digest, 'size': observed.st_size,
                       'identity': fd_identity(observed)}
            expected = self.record['regular'] if self.record['kind'] == 'regular' else self.record['link']['resolved_regular']
            if regular != expected or (self.record['kind'] == 'regular') != (not actual_steps):
                fail('NativeFileMismatch')
            if actual_steps and actual_steps != self.record['link']['steps']:
                fail('NativeLinkMismatch')
            return
        fail('NativeLinkCycleOrLimit')

    def recheck(self):
        for parent, name, raw, before in self.links:
            parent.verify_name()
            current = os.stat(name, dir_fd=parent.fd, follow_symlinks=False)
            if stable_stat(current) != before or os.readlink(os.fsencode(name), dir_fd=parent.fd) != raw:
                fail('InputMutation', 'native_link', True)
        self.parent.verify_name()
        digest, st = hash_fd(self.target, LIMITS['tool_file'], self.budget)
        expected = self.record['regular'] if self.record['kind'] == 'regular' else self.record['link']['resolved_regular']
        current_path = os.stat(self.target_name, dir_fd=self.parent.fd, follow_symlinks=False)
        if stable_stat(st) != self.target_before or (current_path.st_dev, current_path.st_ino) != (st.st_dev, st.st_ino) or digest != expected['sha256']:
            fail('InputMutation', 'native_target', True)

    def close(self):
        if self.target is not None:
            os.close(self.target)
            self.target = None
        if self.parent is not None:
            self.parent.close()
            self.parent = None
        for parent, _, _, _ in self.links:
            parent.close()
        self.links = []


class NativeInputs:
    def __init__(self, spec, budget):
        self.spec, self.budget = spec, budget
        self.tools, self.roots, self.files = {}, [], []
        self.manifest_fd = None
        try:
            self._open()
        except BaseException:
            self.close()
            raise

    def _inventory(self):
        result = set()
        def descend(cap, prefix, depth):
            if depth > LIMITS['path_depth']:
                fail('NativeInputsUnsupported')
            for name in cap.names():
                self.budget.check()
                relative_path(name)
                path = prefix+'/'+name
                st = os.stat(name, dir_fd=cap.fd, follow_symlinks=False)
                if stat.S_ISDIR(st.st_mode):
                    with cap.directory(name) as child:
                        descend(child, path, depth+1)
                else:
                    if not stat.S_ISREG(st.st_mode) and not stat.S_ISLNK(st.st_mode):
                        fail('NativeInputsUnsupported')
                    result.add(path)
                    if len(result) > LIMITS['tool_entries']:
                        fail('ToolchainLimit')
        for cap in self.roots:
            descend(cap, cap.label, 0)
        return result

    def _open(self):
        for root in self.spec['roots']:
            if not any(_contains(prefix, root) for prefix in PUBLIC_NATIVE_PREFIXES):
                fail('NativePathNotPublic')
            if any(_contains(root, old.label) or _contains(old.label, root) for old in self.roots):
                fail('AliasedNativeRoot')
            cap = Directory.open_absolute(root)
            if cap.identity in [old.identity for old in self.roots]:
                cap.close()
                fail('AliasedNativeRoot')
            self.roots.append(cap)
        self.manifest_fd = open_absolute_file(self.spec['manifest_path'], LIMITS['manifest_json'], self.budget)
        raw = read_fd(self.manifest_fd, LIMITS['manifest_json'], self.budget)
        if hashlib.sha256(raw).hexdigest() != self.spec['manifest_sha256']:
            fail('NativeManifestMismatch')
        self.manifest = validate('native_manifest', decode_json(raw, LIMITS['manifest_json']))
        if self.manifest['roots'] != self.spec['roots']:
            fail('NativeRootMismatch')
        paths = [x['path'] for x in self.manifest['files']]
        if paths != sorted(set(paths)) or set(paths) != self._inventory():
            fail('NativeManifestMembership')
        total = 0
        for record in self.manifest['files']:
            item = NativeFile(record, self.spec['roots'], self.budget)
            self.files.append(item)
            total += os.fstat(item.target).st_size
            if total > LIMITS['tool_total']:
                fail('ToolchainLimit')
        for name in ('cc', 'cxx', 'pkg_config', 'ld', 'ar'):
            self.tools[name] = Executable.pin(self.spec[name], self.budget)
            executable = self.tools[name]
            native_public(executable.path, self.spec['roots'])
            if executable.path not in paths:
                fail('NativeToolNotFingerprinted')
            if os.pread(executable.fd, 4, 0) != b'\x7fELF':
                fail('NativeCompilerWrapperUnsupported')
        self.digest = domain_hash('native', self.manifest)

    def recheck(self):
        if hashlib.sha256(read_fd(self.manifest_fd, LIMITS['manifest_json'], self.budget)).hexdigest() != self.spec['manifest_sha256']:
            fail('InputMutation', 'native_manifest', True)
        for root in self.roots:
            root.verify_name()
        if {x['path'] for x in self.manifest['files']} != self._inventory():
            fail('InputMutation', 'native_membership', True)
        for item in self.files:
            item.recheck()
        for tool in self.tools.values():
            tool.recheck(self.budget)
        return self.digest

    def inspect(self, supervisor, cwd, env):
        for name, executable in self.tools.items():
            raw, _ = _command_bytes(supervisor, executable, ['--version'], cwd, env, self.budget)
            if not raw.strip():
                fail('NativeInspectionUnknown')
            raw.decode('utf-8')
        observations = []
        for name, language in (('cc', 'c'), ('cxx', 'c++')):
            executable = self.tools[name]
            _, result = _command_bytes(supervisor, executable, ['-E', '-x', language, '-v', '/dev/null'], cwd, env, self.budget)
            fd = supervisor.store.root.file(result.stderr)
            try:
                text = read_fd(fd, LIMITS['stderr'], self.budget).decode('utf-8')
            finally:
                os.close(fd)
            match = re.search(r'#include <\.\.\.> search starts here:\n(.*?)End of search list\.', text, re.S)
            if match is None:
                fail('NativeInspectionUnknown')
            for path in match.group(1).splitlines():
                path = os.path.normpath(path.strip())
                native_public(path, self.spec['roots'])
                observations.append(path)
            raw, _ = _command_bytes(supervisor, executable, ['-print-search-dirs'], cwd, env, self.budget)
            lines = raw.decode('utf-8').splitlines()
            libraries = [x.split('=', 1)[1] for x in lines if x.startswith('libraries: =')]
            if len(libraries) != 1:
                fail('NativeInspectionUnknown')
            for path in libraries[0].split(':'):
                normalized = os.path.normpath(path)
                native_public(normalized, self.spec['roots'])
                observations.append(normalized)
            for argument in ('-print-prog-name=cc1' if name == 'cc' else '-print-prog-name=cc1plus', '-print-file-name=libgcc.a'):
                raw, _ = _command_bytes(supervisor, executable, [argument], cwd, env, self.budget)
                observed = raw.decode('utf-8').strip()
                native_public(observed, self.spec['roots'])
                if observed not in {x['path'] for x in self.manifest['files']}:
                    fail('NativeAuxiliaryUnknown')
                observations.append(observed)
            raw, _ = _command_bytes(supervisor, executable, ['-print-sysroot'], cwd, env, self.budget)
            sysroot = raw.decode('utf-8').strip()
            if sysroot:
                native_public(os.path.normpath(sysroot), self.spec['roots'])
        raw, _ = _command_bytes(supervisor, self.tools['pkg_config'], ['--variable=pc_path', 'pkg-config'], cwd, env, self.budget)
        for path in raw.decode('utf-8').strip().split(':'):
            native_public(os.path.normpath(path), self.spec['roots'])
        return observations

    def close(self):
        for item in self.files:
            item.close()
        self.files = []
        for executable in self.tools.values():
            executable.close()
        self.tools = {}
        for cap in self.roots:
            cap.close()
        self.roots = []
        if self.manifest_fd is not None:
            os.close(self.manifest_fd)
            self.manifest_fd = None


def not_started_lane(index):
    return {'index': index, 'state': 'not_started', 'source_before': None,
            'source_after': None, 'seed_before': None, 'seed_after': None,
            'toolchain_before': None, 'toolchain_after': None,
            'closure_sha256': None, 'cache_initially_empty': None,
            'joined': None, 'targets': []}


def evidence(state='not_started', digest=None, reason='not_started'):
    return {'state': state, 'sha256': digest,
            'reason': None if state == 'present_verified' else reason}


def empty_receipt(request, raw):
    return {'schema_version': 2, 'request_raw_sha256': hashlib.sha256(raw).hexdigest(),
            'request_canonical_sha256': domain_hash('request.canonical', request),
            'status': 'incomplete', 'source': evidence(), 'origins': [],
            'flavour': request['flavour'],
            'host': {'system': platform.system(), 'architecture': platform.machine()},
            'environment': {'go_version': GO_VERSION, 'goos': 'linux', 'goarch': 'arm64',
                'goarm64': 'v8.0', 'cgo': 1 if request['flavour'] == 'native-linux-arm64' else 0,
                'goexperiment': '', 'vcs': 'disabled_archive', 'network_isolation': 'not_established',
                'image': request['image']},
            'toolchain': evidence(), 'native': evidence(), 'offline': evidence(),
            'supervisor': {'state': 'not_started', 'all_joined': None, 'commands': 0, 'reason': 'not_started'},
            'lanes': [not_started_lane(0), not_started_lane(1)], 'artifacts': [],
            'comparison': 'not_established',
            'limitations': ['same_host_recorded_inputs_only', 'not_hermetic',
                'not_runtime_acceptance', 'network_isolation_not_established',
                'observed_limits_not_physical_quotas', 'no_built_artifact_executed',
                'no_D1_resource_or_release_acceptance'],
            'failure': {'code': 'NotCompleted', 'phase': 'startup'},
            'cleanup': {'state': 'unconfirmed', 'possible_writers': False},
            'output_manifest_sha256': 'f'*64}


def validate_go_probe(raw):
    # Same canonical version as the closed receipt/schema and artifact reader.
    match = re.fullmatch(rb'go version '+re.escape(GO_VERSION.encode('ascii'))+
                         rb' linux/([a-z0-9]+)\n', raw)
    if match is None:
        fail('GoVersionMismatch', 'toolchain')
    return match.group(1).decode('ascii')


def parse_artifact_metadata(version_raw, build_id_raw, target):
    try:
        text, build_id = version_raw.decode('utf-8'), build_id_raw.decode('ascii').strip()
    except UnicodeError as exc:
        raise Failure('ArtifactMetadataInvalid') from exc
    lines = text.splitlines()
    if not lines or not lines[0].endswith(': '+GO_VERSION) or not build_id or len(build_id.encode()) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in build_id):
        fail('ArtifactMetadataInvalid')
    settings, path, module = {}, None, None
    for line in lines[1:]:
        parts = line.strip().split('\t')
        if not parts or parts == ['']:
            continue
        kind = parts[0]
        if kind == 'path' and len(parts) == 2 and path is None:
            path = parts[1]
        elif kind == 'mod' and len(parts) in (3, 4) and module is None:
            module = parts[1:]
        elif kind == 'dep' and len(parts) in (3, 4):
            pass
        elif kind == 'build' and len(parts) == 2 and '=' in parts[1]:
            key, value = parts[1].split('=', 1)
            if key in settings:
                fail('ArtifactMetadataInvalid')
            settings[key] = value
        else:
            fail('ArtifactMetadataInvalid')
    if path is None or module is None or not path.endswith(TARGETS[target][1:]):
        fail('ArtifactMetadataInvalid')
    mandatory = {'GOOS': 'linux', 'GOARCH': 'arm64', 'GOARM64': 'v8.0', '-trimpath': 'true'}
    if any(settings.get(k) != value for k, value in mandatory.items()) or any(key.startswith('vcs') for key in settings):
        fail('ArtifactMetadataInvalid')
    return {'build_id': build_id, 'module': module, 'path': path,
            'settings': settings, 'reported_version_state': 'deferred_to_owned_runtime',
            'runtime_exe_state': 'deferred_to_owned_runtime', 'vcs': 'disabled_archive'}


def _source_version(source_cap, budget):
    try:
        fd = source_cap.file('internal/core/config/config.go')
    except FileNotFoundError:
        return {'state': 'unavailable', 'reason': 'source_version_declaration_missing'}
    try:
        raw = read_fd(fd, LIMITS['source_file'], budget)
    finally:
        os.close(fd)
    values = []
    for name in ('Major', 'Minor', 'Build'):
        matches = re.findall(rb'\bClientVersion'+name.encode()+rb'\s*=\s*([0-9]+)\b', raw)
        if len(matches) != 1:
            return {'state': 'unavailable', 'reason': 'source_version_declaration_unknown',
                    'source_sha256': hashlib.sha256(raw).hexdigest()}
        values.append(str(int(matches[0])))
    return {'state': 'source_declared_only', 'value': '.'.join(values),
            'source_sha256': hashlib.sha256(raw).hexdigest(),
            'reported_version_state': 'deferred_to_owned_runtime'}


def safe_delete_contents(cap, budget, depth=0):
    if depth > LIMITS['path_depth']:
        fail('CleanupIncomplete', 'cleanup', True)
    st = os.fstat(cap.fd)
    if st.st_uid != os.getuid():
        fail('CleanupIncomplete', 'cleanup', True)
    os.fchmod(cap.fd, 0o700)
    for name in cap.names():
        budget.check()
        relative_path(name)
        st = os.stat(name, dir_fd=cap.fd, follow_symlinks=False)
        if stat.S_ISDIR(st.st_mode):
            with cap.directory(name) as child:
                if os.fstat(child.fd).st_dev != os.fstat(cap.fd).st_dev:
                    fail('CleanupIncomplete', 'cleanup', True)
                safe_delete_contents(child, budget, depth+1)
            os.rmdir(name, dir_fd=cap.fd)
        else:
            os.unlink(name, dir_fd=cap.fd)  # unlink namespace only; never follow
    os.fsync(cap.fd)


class BuildPair:
    def __init__(self, request, raw_request, store, budget):
        self.request, self.raw, self.store, self.budget = request, raw_request, store, budget
        self.overall = budget
        self.receipt = empty_receipt(request, raw_request)
        self.supervisor = None
        self.resources, self.lanes, self.source_manifests, self.used = [], [], [], []
        self.telemetry_modes = {}
        self.generated, self.go, self.git, self.goroot, self.seed, self.native = [], None, None, None, None, None

    def own(self, resource):
        self.resources.append(resource)
        return resource

    def _generator_identity(self):
        records = []
        paths = [os.path.realpath(__file__), str(Path(__file__).resolve().parent.parent/'docs/design/loadstand-build-provenance.schema.json'), os.path.realpath(sys.executable)]
        for path in paths:
            fd = open_absolute_file(path, LIMITS['tool_file'], self.budget)
            try:
                digest, st = hash_fd(fd, LIMITS['tool_file'], self.budget)
            except BaseException:
                os.close(fd)
                raise
            self.generated.append((fd, digest, stable_stat(st), path))
            records.append({'kind': ('wrapper', 'schema', 'python')[len(records)],
                            'sha256': digest, 'size': st.st_size})
        return {'python_version': platform.python_version(), 'inputs': records}

    def _recheck_generator(self):
        for fd, expected, before, path in self.generated:
            actual, st = hash_fd(fd, LIMITS['tool_file'], self.budget)
            current = os.stat(path, follow_symlinks=False)
            if actual != expected or stable_stat(st) != before or (current.st_dev, current.st_ino) != (st.st_dev, st.st_ino):
                fail('InputMutation', 'generated_tool', True)

    def _prepare(self):
        # FIRST external-launch preflight. Unsupported Darwin never reaches any
        # Git/Go/compiler/metadata invocation or executable discovery command.
        self.supervisor = LinuxSupervisor(self.store, self.budget)
        if self.request['flavour'] == 'native-linux-arm64' and platform.machine() not in ('arm64', 'aarch64'):
            fail('NativeHostMismatch', 'preflight')
        self.store.put('request.json', self.raw, 'request')
        self.store.json('manifests/generated-tool.json', self._generator_identity(), 'toolchain')
        self.git = self.own(Executable.pin(self.request['git']['executable'], self.budget))
        self.go = self.own(Executable.pin(self.request['toolchain']['go'], self.budget))
        for executable in (self.go, self.git):
            if os.pread(executable.fd, 4, 0) != b'\x7fELF':
                fail('ToolExecutableUnsupported', 'preflight')
        self.goroot_cap = self.own(Directory.open_absolute(self.request['toolchain']['goroot']))
        self.goroot = Fingerprint(self.goroot_cap, self.request['toolchain'], self.budget)
        self.own(self.goroot)
        self.receipt['toolchain'] = evidence('present_verified', self.goroot.initial)
        self.store.json('manifests/toolchain.json', self.goroot.manifest, 'toolchain')
        git_dir = self.own(Directory.open_absolute(self.request['git']['directory']))
        git_home = self.own(self.store.root.directory('git-home', create=True))
        provider = GitObjects(self.supervisor, self.git, git_dir, git_home,
                              git_environment(git_home.label), self.budget)
        self.source_plan = SourcePlan(provider, self.request, self.budget)
        for target in self.request['targets']:
            prefix = TARGETS[target][2:]+'/'
            if not any(p.startswith(prefix) and p.endswith('.go') for p in self.source_plan.entries):
                fail('TargetSourceMissing', 'source')
        self.receipt['origins'] = self.source_plan.origins
        # Checksum authority is the exact consumed verified source go.sum blob.
        sums = provider.object(self.source_plan.entries['go.sum'][1], 'blob')
        self.seed = self.own(Seed(self.request['seed'], parse_go_sum(sums), self.budget))
        self.store.json('manifests/seed.json', self.seed.manifest, 'seed')
        self.store.json('manifests/checksum-authority.json',
                        {'sha256': hashlib.sha256(sums).hexdigest(),
                         'origin': self.source_plan.entries['go.sum'][2],
                         'trust': 'uncommitted_source' if self.source_plan.entries['go.sum'][2]['kind'] == 'staged_uncommitted' else 'committed_source'}, 'module_checks')
        if self.request['native']:
            self.native = self.own(NativeInputs(self.request['native'], self.budget))
            self.receipt['native'] = evidence('present_verified', self.native.digest)
            self.store.json('manifests/native.json', self.native.manifest, 'native')
        else:
            self.receipt['native'] = evidence('unavailable', reason='CompileOnly_CGO0')

    def _observe_work(self):
        # Observed sizes, not physical quotas. Files can grow between ticks.
        now_budget = Budget(min(self.budget.end, time.monotonic()+LIMITS['shutdown_seconds']))
        combined = 0
        for lane in self.lanes:
            measure_tree(lane['cache'], now_budget, LIMITS['gocache'])
            combined += measure_tree(lane['root'], now_budget, LIMITS['working']-combined)

    def _before_after(self, lane, manifest, used):
        self.budget.check()
        check_root_names(lane)
        verify_lane_telemetry(lane['home'], self.telemetry_modes[lane['home'].identity], self.budget)
        self._recheck_generator()
        self.git.recheck(self.budget)
        self.go.recheck(self.budget)
        source = check_source(lane['source'], manifest, self.budget)
        seed = verify_seed_destination(self.seed, lane['modules'], self.budget)
        toolchain = self.goroot.verify()
        if self.native:
            self.native.recheck()
        for key in used:
            verify_module_tree(lane['modules'], self.seed.modules[key], self.budget)
        return source, seed, toolchain

    def _use_budget(self, budget):
        self.budget = budget
        self.store.budget = budget
        if self.supervisor:
            self.supervisor.budget = budget
        for resource in (self.seed, self.goroot, self.native):
            if resource:
                resource.budget = budget
        if self.native:
            for item in self.native.files:
                item.budget = budget
        if hasattr(self, 'source_plan'):
            self.source_plan.budget = budget
            self.source_plan.provider.budget = budget

    def _lane(self, number):
        lane_budget = self.overall.child(LIMITS['lane_seconds'])
        self._use_budget(lane_budget)
        try:
            self._lane_body(number, lane_budget)
        finally:
            self._use_budget(self.overall)

    def _lane_body(self, number, lane_budget):
        record = self.receipt['lanes'][number]
        record['state'] = 'incomplete'
        previous = tuple(self.lanes)
        lane = new_lane(self.store.root, ('lane-A', 'lane-B')[number], self.lanes)
        self.telemetry_modes[lane['home'].identity] = initialize_lane_telemetry(lane['home'], lane_budget)
        if any(a.identity == b.identity for old in previous for a in old.values() for b in lane.values()):
            fail('AliasedLaneRoot')
        self.source_manifests.append(self.source_plan.materialize(lane['source']))
        manifest = self.source_manifests[-1]
        digest = source_digest(manifest['files'])
        if number == 0:
            self.receipt['source'] = evidence('present_verified', digest)
        elif digest != self.receipt['source']['sha256']:
            fail('InputMutation', 'source', True)
        self.store.json(f'manifests/lane-{number}-source.json', manifest, 'source')
        self.seed.copy_to(lane['modules'])
        record['cache_initially_empty'] = lane['cache'].names() == []
        if not record['cache_initially_empty']:
            fail('CacheNotEmpty')
        tools_path = str(Path(self.go.path).parent)
        if self.native:
            tools_path += ':'+':'.join(sorted({str(Path(x.path).parent) for x in self.native.tools.values()}))
        env = environment_for_lane(self.request, lane, self.goroot_cap.label, tools_path, self.source_plan.epoch)
        if self.native:
            env.update(CC=self.native.tools['cc'].path, CXX=self.native.tools['cxx'].path,
                       PKG_CONFIG=self.native.tools['pkg_config'].path)
        expected = self._before_after(lane, manifest, set())
        record.update(source_before=expected[0], seed_before=expected[1], toolchain_before=expected[2])
        self.store.json(f'checks/lane-{number}-before.json', record.copy(), 'lane_check')
        version, _ = _command_bytes(self.supervisor, self.go, ['version'], lane['source'], env, lane_budget)
        host_arch = validate_go_probe(version)
        for tool in ('compile', 'link', 'asm', 'cgo'):
            path = f'pkg/tool/linux_{host_arch}/{tool}'
            if path not in {x['path'] for x in self.goroot.manifest['files']}:
                fail('GoToolchainIncomplete')
        if self.native:
            self.native.inspect(self.supervisor, lane['source'], env)
        graph, closure, used = graph_and_closure(self.supervisor, self.go, lane, env,
                                                self.request, self.seed, manifest, lane_budget, self.native)
        self.used.append(used)
        self.store.json(f'manifests/lane-{number}-graph.json', graph, 'graph')
        self.store.json(f'manifests/lane-{number}-closure.json', closure, 'closure')
        record['closure_sha256'] = domain_hash('closure', {'graph_sha256': domain_hash('graph', graph), 'closure': closure})
        artifact_total = 0
        for target in self.request['targets']:
            lane_budget.check()
            output = lane['bin'].label+'/'+target
            argv = (self.go.path, 'build', '-mod=readonly', '-trimpath', '-buildvcs=false',
                    '-ldflags', '-s -w', '-o', output, TARGETS[target])
            result = self.supervisor.run_owned(argv, self.go, lane['source'], env,
                min(LIMITS['build_seconds'], max(0, lane_budget.end-time.monotonic())),
                observe_work=self._observe_work)
            require_command(result)
            fd = lane['bin'].file(target)
            try:
                digest, st = hash_fd(fd, LIMITS['artifact'], lane_budget)
                artifact_total += st.st_size
                if artifact_total > LIMITS['lane_artifacts']:
                    fail('ArtifactLimit')
                os.lseek(fd, 0, os.SEEK_SET)
                destination = f'artifacts/{number}-{target}'
                copied = self.store.create(destination, 'artifact')
                while True:
                    lane_budget.check()
                    data = os.read(fd, MIB)
                    if not data:
                        break
                    _write_all(copied, data, lane_budget)
                stored = self.store.finish(destination)
                if stored['sha256'] != digest or stored['size'] != st.st_size:
                    fail('ArtifactMutation', 'artifact', True)
                # These are metadata readers. The artifact itself is NEVER exec'd.
                buildid, _ = _command_bytes(self.supervisor, self.go, ['tool', 'buildid', output], lane['source'], env, lane_budget)
                metadata, _ = _command_bytes(self.supervisor, self.go, ['version', '-m', output], lane['source'], env, lane_budget)
                parsed = parse_artifact_metadata(metadata, buildid, target)
                if parsed['settings'].get('CGO_ENABLED') != env['CGO_ENABLED']:
                    fail('ArtifactMetadataInvalid')
                self.store.json(f'metadata/{number}-{target}.json', parsed, 'metadata')
                final_digest, final_st = hash_fd(fd, LIMITS['artifact'], lane_budget)
                current_st = os.stat(target, dir_fd=lane['bin'].fd, follow_symlinks=False)
                if final_digest != digest or stable_stat(final_st) != stable_stat(st) or (current_st.st_dev, current_st.st_ino) != (st.st_dev, st.st_ino):
                    fail('ArtifactMutation', 'metadata', True)
                self.receipt['artifacts'].append({'lane': number, 'target': target,
                    'size': st.st_size, 'sha256': digest,
                    'reported_version_state': 'deferred_to_owned_runtime',
                    'runtime_exe_state': 'deferred_to_owned_runtime'})
            finally:
                os.close(fd)
            record['targets'].append(target)
        after = self._before_after(lane, manifest, used)
        record.update(source_after=after[0], seed_after=after[1], toolchain_after=after[2],
                      joined=not self.supervisor.poisoned and self.supervisor.anchor is None,
                      state='complete')
        validate('lane', record)
        self.store.json(f'checks/lane-{number}-after.json', record.copy(), 'lane_check')
        # All A tools joined + complete A evidence is durable before B.
        self.store.json(f'checkpoints/lane-{number}.json', record.copy(), 'checkpoint')
        if number == 0:
            self.store.json('metadata/source-version.json', _source_version(lane['source'], self.budget), 'metadata')

    def _cleanup(self, possible_writers):
        if possible_writers:
            return {'state': 'unconfirmed', 'possible_writers': True}
        if self.request['keep_work']:
            return {'state': 'kept_work', 'possible_writers': False}
        cleanup = Budget(time.monotonic()+LIMITS['delete_seconds'])
        try:
            for lane in self.lanes:
                safe_delete_contents(lane['root'], cleanup)
                name = lane['root'].label.rsplit('/', 1)[1]
                with self.store.root.directory('work') as parent:
                    os.rmdir(name, dir_fd=parent.fd)
                    os.fsync(parent.fd)
            return {'state': 'complete', 'possible_writers': False}
        except (OSError, Failure):
            return {'state': 'incomplete', 'possible_writers': False}

    def execute(self):
        failure = None
        try:
            self._prepare()
            for number in (0, 1):
                self._lane(number)
            # Recheck A's still retained consumed inputs AFTER B, too.
            for number, lane in enumerate(self.lanes):
                self._before_after(lane, self.source_manifests[number], self.used[number])
            self.receipt['offline'] = evidence('present_verified', domain_hash('seed', self.seed.manifest))
            by_lane = [{x['target']: x['sha256'] for x in self.receipt['artifacts'] if x['lane'] == n} for n in (0, 1)]
            if by_lane[0] != by_lane[1]:
                fail('ReproducibilityMismatch', 'comparison')
            self.receipt.update(status='verified_equal', failure=None, comparison='equal')
        except Failure as exc:
            failure = exc
        except (OSError, UnicodeError, zipfile.BadZipFile, KeyError, ValueError) as exc:
            failure = Failure('IOOrEvidenceUnknown', 'execution', True)
        if failure:
            for lane_record in self.receipt['lanes']:
                if lane_record['state'] == 'incomplete':
                    lane_record['state'] = 'incomplete' if failure.incomplete else 'failed'
                    lane_record['joined'] = self.supervisor is not None and self.supervisor.anchor is None and not self.supervisor.poisoned
            self.receipt.update(status='incomplete' if failure.incomplete else 'failed',
                                failure={'code': failure.code, 'phase': failure.phase})
            if failure.code == 'ReproducibilityMismatch':
                self.receipt['comparison'] = 'different'
        possible_writers = self.supervisor is not None and (self.supervisor.poisoned or self.supervisor.anchor is not None)
        if self.supervisor:
            joined = not possible_writers
            self.receipt['supervisor'] = {'state': 'present_verified' if joined else 'unconfirmed',
                'all_joined': joined, 'commands': len(self.supervisor.records),
                'reason': None if joined else 'CleanupUnconfirmed'}
        # Cancellation may not abandon already-joined evidence publication.
        # Storage syscalls remain kernel-I/O observations, not a hard timeout.
        final_end = self.supervisor.cleanup_end if self.supervisor and self.supervisor.cleanup_end else min(self.overall.end, time.monotonic()+LIMITS['delete_seconds'])
        self.store.budget = Budget(final_end)
        if failure:
            self.store.json('failure.json', self.receipt['failure'], 'failure')
        self.store.json('comparison.json', {'state': self.receipt['comparison']}, 'comparison')
        self.store.json('checkpoint.json', {'terminal': False, 'status': self.receipt['status']}, 'checkpoint')
        self.receipt['cleanup'] = self._cleanup(possible_writers)
        if self.receipt['cleanup']['state'] in ('incomplete', 'unconfirmed'):
            self.receipt.update(status='incomplete', failure={'code': 'CleanupIncomplete', 'phase': 'cleanup'})
        manifest = self.store.verified_manifest()
        self.receipt['output_manifest_sha256'] = domain_hash('outputs', manifest)
        self.store.put('artifacts-manifest.json', canonical(manifest), 'checkpoint')
        # Remove from registry only: the self-excluded manifest stays retained.
        self.store.records.pop('artifacts-manifest.json')
        validate_terminal(self.receipt, self.request, self.raw, manifest)
        self.close()  # no more input capabilities/tools after terminal publish
        self.store.publish_receipt(self.receipt)
        return self.receipt

    def close(self):
        for lane in self.lanes:
            for cap in reversed(list(lane.values())):
                cap.close()
        while self.resources:
            self.resources.pop().close()
        for fd, _, _, _ in self.generated:
            os.close(fd)
        self.generated = []


def check_output_scope(request, output):
    absolute_path(output)
    roots = [request['git']['directory'], request['seed']['root'], request['toolchain']['goroot']]
    if request['native'] is not None:
        roots += request['native']['roots']
    files = [request['seed']['manifest_path'], request['toolchain']['manifest_path'],
             request['git']['executable']['path'], request['toolchain']['go']['path']]
    if request['native'] is not None:
        files += [request['native']['manifest_path']]+[request['native'][x]['path'] for x in ('cc', 'cxx', 'pkg_config', 'ld', 'ar')]
    if any(_contains(root, output) or _contains(output, root) for root in roots) or any(_contains(output, path) for path in files):
        fail('AliasedInputOutputRoot', 'preflight')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--request', required=True)
    parser.add_argument('--output', required=True)
    items = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(items)
    if len(items) != 4 or {items[0], items[2]} != {'--request', '--output'}:
        parser.error('exactly one --request value and one --output value are required')
    budget, store, pair = Budget(), None, None
    old_handlers = {}
    try:
        fd = open_absolute_file(args.request, LIMITS['request_json'], budget)
        try:
            raw = read_fd(fd, LIMITS['request_json'], budget)
        finally:
            os.close(fd)
        request = validate('request', decode_json(raw))
        # Even malformed platform/toolchain setup cannot reach an external tool
        # on Darwin. Request validation itself is a pure local operation.
        LinuxSupervisor.preflight()
        check_output_scope(request, args.output)
        store = OutputStore(args.output, budget)
        for sig in (signal.SIGTERM, signal.SIGINT):
            old_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, lambda *_: budget.cancelled.__setitem__(0, True))
        pair = BuildPair(request, raw, store, budget)
        receipt = pair.execute()
        print(json.dumps({'status': receipt['status'], 'receipt': 'receipt.json'}, sort_keys=True))
        return 0 if receipt['status'] == 'verified_equal' else 2
    except (Failure, OSError, UnicodeError) as exc:
        code = exc.code if isinstance(exc, Failure) else 'IOOrEvidenceUnknown'
        print(json.dumps({'status': 'incomplete', 'failure': code, 'receipt': 'absent_or_unconfirmed'}, sort_keys=True), file=sys.stderr)
        return 2
    finally:
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        if pair:
            pair.close()
        if store:
            store.close()



DEFS.update({
    'command_signal': {'signal': E('SIGTERM', 'SIGKILL'), 'outcome': E('sent', 'gone', 'error')},
    'command': {'schema_version': E(2), 'sequence': I(100000, 1),
                 'argv': A(S(), 64, 1), 'stdout': P, 'stderr': P,
                 'exit_code': N(I(255)), 'term_signal': N(I(64, 1)),
                 'joined': B, 'failure': N(S()),
                 'signals': A(R('command_signal'), 2)},
})


def _image(value):
    if (value['state'] == 'absent_native_host') != (value['digest'] is None):
        fail('ImageDiscriminant')


def _command(value):
    if any(len(x.encode()) > LIMITS['argv'] or any(ord(c) < 32 or ord(c) == 127 for c in x) for x in value['argv']):
        fail('UnsafeArgv')
    if value['joined']:
        if (value['exit_code'] is None) == (value['term_signal'] is None):
            fail('CommandTerminalDiscriminant')
    elif value['exit_code'] is not None or value['term_signal'] is not None or value['failure'] is None:
        fail('CommandTerminalDiscriminant')
    if len({x['signal'] for x in value['signals']}) != len(value['signals']):
        fail('DuplicateSignal')


SEMANTIC.update(image=_image, command=_command)


def validate_terminal(receipt, request, raw_request, outputs):
    validate('receipt', receipt)
    validate('request', request)
    validate('outputs', outputs)
    if canonical(validate('request', decode_json(raw_request))) != canonical(request):
        fail('RequestBinding')
    if receipt['request_raw_sha256'] != hashlib.sha256(raw_request).hexdigest() or receipt['request_canonical_sha256'] != domain_hash('request.canonical', request):
        fail('RequestBinding')
    if receipt['flavour'] != request['flavour'] or receipt['environment']['image'] != request['image']:
        fail('RequestBinding')
    if receipt['output_manifest_sha256'] != domain_hash('outputs', outputs):
        fail('OutputManifestBinding')
    if receipt['status'] == 'verified_equal':
        for lane in receipt['lanes']:
            if set(lane['targets']) != set(request['targets']):
                fail('RequestedTargetSet')
        roles = {x['role'] for x in outputs['files'] if x['completeness'] == 'complete'}
        required = {'request', 'source', 'seed', 'toolchain', 'graph', 'closure',
                    'module_checks', 'artifact', 'metadata', 'lane_check',
                    'command_stdout', 'command_stderr', 'command_record', 'comparison', 'checkpoint'}
        if request['native']:
            required.add('native')
        if not required <= roles:
            fail('RequiredEvidenceMissing')
        files = {x['path']: x for x in outputs['files']}
        for artifact in receipt['artifacts']:
            path = 'artifacts/'+str(artifact['lane'])+'-'+artifact['target']
            recorded = files.get(path)
            if recorded is None or recorded['role'] != 'artifact' or recorded['completeness'] != 'complete' or (recorded['sha256'], recorded['size']) != (artifact['sha256'], artifact['size']):
                fail('ArtifactEvidenceBinding')
        raw_records = [x for x in outputs['files'] if x['role'] == 'request']
        if len(raw_records) != 1 or raw_records[0]['sha256'] != receipt['request_raw_sha256'] or raw_records[0]['size'] != len(raw_request):
            fail('RequestBinding')
    return receipt




def _zip_central_bound(fd, size):
    tail_start = max(0, size-65557)
    tail = os.pread(fd, size-tail_start, tail_start)
    offset = tail.rfind(b'PK\x05\x06')
    if offset < 0 or offset+22 > len(tail):
        fail('ModuleZipInvalid')
    values = struct.unpack('<4s4H2LH', tail[offset:offset+22])
    _, disk, directory_disk, disk_count, count, central_size, central_start, comment = values
    if disk or directory_disk or offset+22+comment != len(tail):
        fail('ModuleZipInvalid')
    if count == 65535 or central_size == 0xffffffff or central_start == 0xffffffff:
        absolute = tail_start+offset
        locator = os.pread(fd, 20, absolute-20)
        if len(locator) != 20 or locator[:4] != b'PK\x06\x07':
            fail('ModuleZipInvalid')
        _, disk64, pos64, disks = struct.unpack('<4sLQL', locator)
        header = os.pread(fd, 56, pos64)
        if disk64 or disks != 1 or len(header) != 56 or header[:4] != b'PK\x06\x06':
            fail('ModuleZipInvalid')
        _, record_size, _, _, d1, d2, disk_count, count, central_size, central_start = struct.unpack('<4sQ2H2L4Q', header)
        if record_size < 44 or d1 or d2 or disk_count != count:
            fail('ModuleZipInvalid')
    if count > LIMITS['zip_entries'] or disk_count != count:
        fail('ExpansionLimit')
    if central_start+central_size > size:
        fail('ModuleZipInvalid')
    # Bound the ACTUAL central inventory before ZipFile allocates its list;
    # the EOCD count is attacker-controlled and is not the membership authority.
    position, actual_count, end = central_start, 0, central_start+central_size
    while position < end:
        header = os.pread(fd, 46, position)
        if len(header) != 46 or header[:4] != b'PK\x01\x02':
            fail('ModuleZipInvalid')
        name_size, extra_size, comment_size = struct.unpack_from('<3H', header, 28)
        actual_count += 1
        if actual_count > LIMITS['zip_entries'] or name_size > LIMITS['path']*2:
            fail('ExpansionLimit')
        position += 46+name_size+extra_size+comment_size
        if position > end:
            fail('ModuleZipInvalid')
    if position != end or actual_count != count:
        fail('ModuleZipInvalid')
    return count


def _zip_entry_hash(fd, entry, budget):
    header = os.pread(fd, 30, entry.header_offset)
    if len(header) != 30 or header[:4] != b'PK\x03\x04':
        fail('ModuleZipInvalid')
    (_, _, flags, compression, _, _, _, _, _, name_size, extra_size) = struct.unpack('<4s5H3L2H', header)
    if flags != entry.flag_bits or compression != entry.compress_type:
        fail('ModuleZipInvalid')
    try:
        actual_name = os.pread(fd, name_size, entry.header_offset+30).decode('utf-8')
    except UnicodeError as exc:
        raise Failure('ModuleZipInvalid') from exc
    if actual_name != entry.orig_filename:
        fail('ModuleZipInvalid')
    position = entry.header_offset+30+name_size+extra_size
    if position+entry.compress_size > os.fstat(fd).st_size:
        fail('ModuleZipInvalid')
    digest, actual, crc, remaining = hashlib.sha256(), 0, 0, entry.compress_size
    inflater = zlib.decompressobj(-15) if compression == zipfile.ZIP_DEFLATED else None
    while remaining:
        budget.check()
        chunk = os.pread(fd, min(MIB, remaining), position)
        if not chunk:
            fail('ModuleZipSize')
        remaining -= len(chunk)
        position += len(chunk)
        data = chunk
        while data:
            budget.check()
            old = data
            if inflater:
                try:
                    piece = inflater.decompress(data, min(MIB, entry.file_size+1-actual))
                except zlib.error as exc:
                    raise Failure('ModuleZipInvalid') from exc
                data = inflater.unconsumed_tail
            else:
                piece, data = data, b''
            actual += len(piece)
            if actual > entry.file_size:
                fail('ExpansionLimit')
            digest.update(piece)
            crc = zlib.crc32(piece, crc)
            if inflater and inflater.eof and (inflater.unused_data or remaining or data):
                fail('ModuleZipSize')
            if data == old and not piece:
                fail('ModuleZipSize')
    if inflater and not inflater.eof:
        fail('ModuleZipSize')
    if actual != entry.file_size or crc != entry.CRC:
        fail('ModuleZipSize')
    return digest.hexdigest()


if __name__ == '__main__':
    raise SystemExit(main())
