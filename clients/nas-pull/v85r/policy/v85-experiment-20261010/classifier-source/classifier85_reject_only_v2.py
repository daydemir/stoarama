"""Frozen rejection-only successor: missing capture identity is not positive proof.

Original policy and first successor remain immutable. Genuine source/frame
provenance and local decoded integrity remain mandatory for every override.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

PATH = Path(__file__).parent/'classifier85_successor.py'
BASE_SHA = '5da19896f8b5852fc5404bc2295c738e6f2cb490cf607eef7803eaf9a6ac6a39'
if hashlib.sha256(PATH.read_bytes()).hexdigest() != BASE_SHA:
    raise RuntimeError('First successor differs')
spec = importlib.util.spec_from_file_location('frozen85_negative_successor', PATH)
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)
C = S.C
VERSION = 'v85-qualified-negative-unknown-capture-successor-v2'


def decision(row, bad_sources=(), human_pairs=()):
    out = S.decision(row, bad_sources, human_pairs)
    out.update(version=VERSION, reject_only_unknown_identity_override=False)
    negative = out['successor_negative_status']
    safety = out.get('pair_safety_status', 'UNKNOWN_capture_identity')
    cadence = out.get('decoded_cadence_status', 'UNKNOWN_decoded_cadence')
    stationary = negative == 'REJECT_stationary_severe_seam_forecast'
    cadence_usable = cadence == 'PASS_native_decoded_uniform_cadence' or (
        stationary and cadence == 'UNKNOWN_variable_decoded_cadence_no_PTS_normalization')
    if (negative.startswith('REJECT_') and
            out.get('source_integrity_status') == 'PASS_source_local_integrity' and
            out.get('spatial_provenance_status') == 'PASS_bound_spatial_diagnostic' and
            safety.startswith(('PASS_', 'UNKNOWN_')) and cadence_usable and
            not out.get('status', '').startswith(('HOLD_', 'REJECT_'))):
        out.update(nomination=False, spatial_nomination=False,
                   successor_spatial_reject_override=True,
                   reject_only_unknown_identity_override=safety.startswith('UNKNOWN_') or cadence.startswith('UNKNOWN_'),
                   status=negative)
    return out


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit('usage: classifier85_reject_only_v2.py BAD_SOURCE_INVENTORY HUMAN_ORDERED_VETO < rows.jsonl')
    bad, human = C.load_vetoes(sys.argv[1], sys.argv[2])
    for line in sys.stdin:
        if line.strip():
            print(json.dumps(decision(json.loads(line), bad, human), allow_nan=False))
