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


def decide(c, diagnostic, bindings, bad, human, visual):
    if any(b['sha256'] in bad for b in bindings):
        return False, 'REJECT_known_whole_source'
    if tuple(b['sha256'] for b in bindings) in human:
        return False, 'REJECT_human_ordered_source_pair'
    key = tuple((b['sha256'], b['size_bytes']) for b in bindings)
    if key in visual:
        return False, 'REJECT_qualified_visual6'
    if not diagnostic:
        return False, 'UNKNOWN_missing_features'
    result = c.decision(dict(source_bindings=bindings, diagnostic_window20=diagnostic), bad, human)
    return result['nomination'], {'RESEARCH_nomination': APPROVED, 'RESEARCH_not_nominated': 'SPLIT_frozen_score_reject'}.get(result['status'], result['status'])


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
    nomination, status = decide(c, d, bindings, bad, human, visual)
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
