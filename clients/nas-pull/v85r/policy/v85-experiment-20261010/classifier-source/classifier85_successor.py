"""Separate frozen V85 research successor: qualified spatial negatives veto K.

Zero-flow evidence never supplies a positive nomination. Preview diagnostics
can exercise the negative predicate but cannot satisfy raw source provenance.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

PATH = Path(__file__).parent / 'classifier85.py'
BASE_SHA = '027340b91d5518e08dcf76d4cbb0534226ddb786008fef1d09af7a18582362c7'
if hashlib.sha256(PATH.read_bytes()).hexdigest() != BASE_SHA:
    raise RuntimeError('Frozen V85 original differs')
spec = importlib.util.spec_from_file_location('frozen85_original', PATH)
C = importlib.util.module_from_spec(spec)
spec.loader.exec_module(C)
VERSION = 'v85-qualified-spatial-negative-successor-v1'
PARAMETERS = dict(stationary_severe_residual_floor=1., stationary_severe_residual_factor=8.,
                  minimum_texture=2., minimum_novelty=1., minimum_heldout_pixels=100,
                  stationary_positive_authorization=False, thresholds_calibrated=False)


def stationary_negative_status(spatial):
    if not isinstance(spatial, dict):
        return 'UNKNOWN_stationary_diagnostic_absent'
    if spatial.get('version') != 'v85-spatial-translation-diagnostic-v1' or spatial.get('status') != 'diagnostic_only':
        return 'UNKNOWN_stationary_diagnostic_version'
    fields = ('forward_heldout_residual_mad', 'backward_heldout_residual_mad',
              'internal_heldout_residual_mad', 'texture_gradient_mad',
              'minimum_spatial_novelty_margin', 'direction_speed_disagreement',
              'max_acceleration_pixels_per_frame_squared', 'boundary_unwarped_mad')
    if any(not C.number(spatial.get(k)) for k in fields):
        return 'UNKNOWN_stationary_malformed'
    velocities = ('tail_dx_pixels_per_frame', 'tail_dy_pixels_per_frame',
                  'head_dx_pixels_per_frame', 'head_dy_pixels_per_frame')
    if any(not C.number(spatial.get(k), -float('inf')) or spatial[k] != 0 for k in velocities):
        return 'UNKNOWN_not_stationary'
    if spatial.get('motion_observable') is not False or spatial.get('ambiguities') != ['no_observable_motion']:
        return 'UNKNOWN_stationary_other_ambiguity'
    if (spatial['texture_gradient_mad'] < 2 or spatial['minimum_spatial_novelty_margin'] < 1 or
            type(spatial.get('heldout_pixels')) is not int or spatial['heldout_pixels'] < 100):
        return 'UNKNOWN_stationary_texture_novelty_or_support'
    f = spatial['forward_heldout_residual_mad']; b = spatial['backward_heldout_residual_mad']
    interior = spatial['internal_heldout_residual_mad']
    if abs(f - b) > max(.5, .5 * interior):
        return 'UNKNOWN_stationary_forecast_disagreement'
    # Require BOTH independently forecast residuals severe relative to their
    # ordinary within-side noise, rather than a single inconsistent endpoint.
    if min(f, b) > max(1., 8 * interior):
        return 'REJECT_stationary_severe_seam_forecast'
    return 'UNKNOWN_stationary_no_severe_discontinuity'


def negative_diagnostic_status(spatial):
    stationary = stationary_negative_status(spatial)
    if stationary.startswith('REJECT_'):
        return stationary
    return C.spatial_status(spatial)


def decision(row, bad_sources=(), human_pairs=()):
    out = C.decision(row, bad_sources, human_pairs)
    out.update(version=VERSION, successor_spatial_reject_override=False,
               successor_negative_status=negative_diagnostic_status(row.get('spatial_temporal_diagnostic')))
    # This is a rejection override only. No absence of rejection becomes proof
    # of continuity. Original baselines and original policy result remain legible.
    out['original85_nomination'] = out['nomination']
    required = dict(source_integrity_status='PASS_source_local_integrity',
                    spatial_provenance_status='PASS_bound_spatial_diagnostic',
                    decoded_cadence_status='PASS_native_decoded_uniform_cadence',
                    pair_safety_status='PASS_capture_metadata_only')
    if (out['successor_negative_status'].startswith('REJECT_') and
            all(out.get(k) == v for k, v in required.items()) and
            not out.get('status', '').startswith(('HOLD_', 'REJECT_'))):
        out.update(nomination=False, spatial_nomination=False,
                   successor_spatial_reject_override=True, status=out['successor_negative_status'])
    return out


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit('usage: classifier85_successor.py BAD_SOURCE_INVENTORY HUMAN_ORDERED_VETO < rows.jsonl')
    bad, human = C.load_vetoes(sys.argv[1], sys.argv[2])
    for line in sys.stdin:
        if line.strip():
            print(json.dumps(decision(json.loads(line), bad, human), allow_nan=False))
