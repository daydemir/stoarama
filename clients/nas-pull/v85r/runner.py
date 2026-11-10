"""V85r measurement adapter. Executed in an isolated process, no network/DB."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
APPROVED = 'APPROVED85_nomination_requires_runtime_native_join_checks'


def classifier():
    # Preserve every recovered source byte and historical import hash. The
    # remapping lives only in this short-lived child, never the client process.
    def mapped_args(args):
        if args and str(args[0]).startswith('/tmp/v89-experiment-20261009'):
            return (str(ROOT/'policy'/str(args[0])[5:]), *args[1:])
        return args
    original_init, original_new = Path.__init__, Path.__new__
    # pathlib initializes paths in __new__ through 3.11, __init__ thereafter.
    if original_init is object.__init__:
        def mapped_new(cls, *args, **kwargs):
            return original_new(cls, *mapped_args(args), **kwargs)
        Path.__new__ = staticmethod(mapped_new)
    else:
        def mapped_init(self, *args):
            original_init(self, *mapped_args(args))
        Path.__init__ = mapped_init
    try:
        p = ROOT/'policy/v85-experiment-20261010/classifier-source/classifier85_reject_only_v2.py'
        spec = importlib.util.spec_from_file_location('v85r_classifier', p)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        Path.__init__, Path.__new__ = original_init, staticmethod(original_new)


def decide(c, row, bindings, bad, human, visual):
    if any(b['sha256'] in bad for b in bindings):
        return False, 'REJECT_known_whole_source'
    if tuple(b['sha256'] for b in bindings) in human:
        return False, 'REJECT_human_ordered_source_pair'
    key = tuple((b['sha256'], b['size_bytes']) for b in bindings)
    if key in visual:
        return False, 'REJECT_qualified_visual6'
    if not isinstance(row, dict) or row.get('source_bindings') != bindings:
        return False, 'UNKNOWN_missing_authenticated_evidence'
    diagnostic = row.get('diagnostic_window20')
    pair = row.get('pair')
    replay = row.get('research_packet_replay')
    audio = row.get('research_audio_inventory')
    if (not isinstance(pair, dict) or
            not isinstance(replay, dict) or type(replay.get('original_replay_hold')) is not bool or
            not isinstance(audio, list) or len(audio) != 2 or
            any(not isinstance(a, dict) or type(a.get('has_audio')) is not bool for a in audio) or
            not isinstance(diagnostic, dict) or
            [diagnostic.get('prev_clip_id'), diagnostic.get('next_clip_id')] != [b['clip_id'] for b in bindings] or
            any(not isinstance(pair.get(side), dict) or
                any(pair[side].get(k) != binding[k] for k in ('sha256', 'size_bytes'))
                for side, binding in zip(('prev', 'next'), bindings))):
        return False, 'UNKNOWN_missing_authenticated_evidence'
    result = c.decision(row, bad, human)
    # The recovered base also contains a spatial-positive research route.
    # Preserve every qualified negative, but admit only the pinned K baseline.
    if result['status'].startswith(('REJECT_', 'HOLD_')):
        return False, result['status']
    spatial = row.get('spatial_temporal_diagnostic')
    if (result.get('source_integrity_status') != 'PASS_source_local_integrity' or
            result.get('spatial_provenance_status') != 'PASS_bound_spatial_diagnostic' or
            not isinstance(spatial, dict) or spatial.get('status') != 'diagnostic_only' or
            spatial.get('version') != 'v85-spatial-translation-diagnostic-v1' or
            result.get('spatial_status') in ('UNKNOWN_spatial_malformed', 'UNKNOWN_spatial_version_or_status') or
            result.get('decoded_cadence_status') not in ('PASS_native_decoded_uniform_cadence',
                'UNKNOWN_variable_decoded_cadence_no_PTS_normalization')):
        return False, 'UNKNOWN_missing_authenticated_evidence'
    nomination = result['baselineK_nomination']
    result['status'] = 'RESEARCH_nomination' if nomination else 'RESEARCH_not_nominated'
    return nomination, {'RESEARCH_nomination': APPROVED, 'RESEARCH_not_nominated': 'SPLIT_frozen_score_reject'}.get(result['status'], result['status'])


def has_decode_error(value):
    if isinstance(value, dict):
        return any(('decode_error' in k and bool(v)) or has_decode_error(v) for k, v in value.items())
    if isinstance(value, list):
        return any(has_decode_error(v) for v in value)
    return False


def resolve(nomination, status, keys, frozen=None, decoder_errors=False, missing=False):
    """A frozen exact identity is authoritative only after current safety gates.

    Zero additional decoder errors is deliberately stricter than October's
    tolerances. No unknown/missing observation can promote a frozen positive.
    """
    out = dict(policy='v85r', nomination=nomination, status=status, join=False, frozen=False)
    if missing or decoder_errors or not isinstance(nomination, bool):
        return dict(out, decision='hold', reason='missing_input_or_decoder_error')
    if frozen is not None:
        if frozen.get('source_key') != keys:
            return dict(out, decision='hold', reason='frozen_identity_mismatch')
        if frozen.get('status') != status or frozen.get('nomination') is not nomination:
            return dict(out, decision='hold', reason='october_decision_difference')
        out['frozen'] = True
        nomination, status = frozen['nomination'], frozen['status']
    if status == APPROVED and nomination is True:
        return dict(out, decision='join', join=True, reason='exact_october' if out['frozen'] else 'rebuilt_measurement')
    if status.startswith(('REJECT_', 'SPLIT_')) and nomination is False:
        return dict(out, decision='split', reason='policy_negative')
    return dict(out, decision='hold', reason='ambiguous_status')


def run(request):
    c = classifier()
    bad, human = c.C.load_vetoes(ROOT/'source71.json', ROOT/'human12.json')
    visual = {tuple((b['sha256'], b['size_bytes']) for b in v['source_bindings'])
              for v in json.loads((ROOT/'visual6.json').read_text())['cases']}
    a, b = request['prev'], request['next']
    bindings = [dict(clip_id=z['clip_id'], sha256=z['sha256'], size_bytes=z['size_bytes']) for z in (a, b)]
    request = dict(request, bad_sources=sorted(bad), human_pairs=[dict(PrevSHA=a, NextSHA=b) for a,b in sorted(human)])
    env = dict(os.environ, GOMAXPROCS='1', V85R_FFMPEG=request['tools']['ffmpeg'], V85R_FFPROBE=request['tools']['ffprobe'])
    measured = subprocess.run([str(ROOT/'remeasure-linux-amd64')], input=json.dumps(request),
                              capture_output=True, text=True, timeout=185, env=env, check=True)
    native = json.loads(measured.stdout)
    d = native.get('diagnostic_window20')
    if d and d.get('reason') in ('prev_clip_cut_short','next_clip_cut_short','content_span_mismatch'):
        request['actual_span'] = True
        measured = subprocess.run([str(ROOT/'remeasure-linux-amd64')], input=json.dumps(request),
                                  capture_output=True, text=True, timeout=185, env=env, check=True)
        native = json.loads(measured.stdout)
        d = native.get('diagnostic_window20')
    row = native.get('evidence_row')
    if isinstance(row, dict) and isinstance(row.get('spatial_provenance'), dict):
        # Canonical Python JSON is the pinned classifier's diagnostic hash
        # format. Only locally measured producer output reaches this adapter.
        row['spatial_provenance']['diagnostic_sha256'] = hashlib.sha256(json.dumps(
            row.get('spatial_temporal_diagnostic'), sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    nomination, status = decide(c, row, bindings, bad, human, visual)
    out = resolve(nomination, status, [[z['clip_id'],z['sha256'],z['size_bytes']] for z in bindings],
                  request.get('frozen'), has_decode_error(native), not d or bool(native.get('error') or native.get('source_integrity_error')))
    out['diagnostic_sha256'] = hashlib.sha256(json.dumps(d, sort_keys=True, separators=(',',':'), allow_nan=False).encode()).hexdigest()
    return out


if __name__ == '__main__':
    sys.dont_write_bytecode = True
    try:
        result = run(json.loads(Path(sys.argv[1]).read_text()))
    except Exception:
        result = dict(policy='v85r', join=False, nomination=False, status='UNKNOWN_measurement_failed', decision='hold', reason='measurement_failed')
    print(json.dumps(result, allow_nan=False))
