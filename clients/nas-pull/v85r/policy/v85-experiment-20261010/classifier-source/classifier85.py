"""Frozen-feature research nominations plus a separately qualified spatial lane.

No physical-clock proof, trimming authorization, queue admission, or production
decision is created here. Baseline nominations remain distinct from qualification.
"""
import hashlib
import importlib.util
import json
import math
import statistics
from pathlib import Path
import sys

VERSION = 'v85-spatial-independent-side-research-v1'
ROOT = Path('/tmp/v89-experiment-20261009')
BASE79_PATH = ROOT / 'dependencies/experimental-79/classifier79.py'
BASE79_SHA = 'd79c05e1c13f502ef09feffbc2324cf07596b438f5d4622bd5b4bf262445ced6'
K_PATH = ROOT / 'dependencies/experimental-89/revision-k/classifier89_revision_k.py'
K_SHA = '947277fa0a96e679e0a8122c8f135c9cc38b5b35b281a7805271a32009184837'
PRODUCER_SHA = 'f727ff7624d6ce99f8fe67e25529931ad3ca1fad5c4dc738f141fe735b0932bf'
PARAMETERS = dict(texture_min=2., novelty_min=1., minimum_heldout_pixels=100,
                  internal_residual_max=4., residual_floor=1., residual_factor=2.,
                  forward_backward_floor=.5, forward_backward_factor=.5,
                  side_speed_disagreement_max=1., acceleration_max=1.,
                  maximum_absolute_velocity_exclusive=4.,
                  thresholds_calibrated=False, research_only=True)


def pinned_module(path, sha, name):
    if hashlib.sha256(path.read_bytes()).hexdigest() != sha:
        raise RuntimeError('Pinned baseline differs: ' + str(path))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BASE79 = pinned_module(BASE79_PATH, BASE79_SHA, 'v85_frozen79')
K = pinned_module(K_PATH, K_SHA, 'v85_frozenK')


def number(x, minimum=0):
    return type(x) in (int, float) and math.isfinite(x) and x >= minimum


def bindings_valid(b):
    return (isinstance(b, list) and len(b) == 2 and
            all(isinstance(s, dict) and isinstance(s.get('sha256'), str) and
                len(s['sha256']) == 64 and all(c in '0123456789abcdef' for c in s['sha256']) and
                type(s.get('size_bytes')) is int and s['size_bytes'] > 0 for s in b))


def source_key(b):
    return tuple((s['sha256'], s['size_bytes']) for s in b)


def load_vetoes(bad_path, human_path):
    bad = json.loads(Path(bad_path).read_text())
    human = json.loads(Path(human_path).read_text())
    return ({s['source_binding']['sha256'] for s in bad['sources']},
            {tuple(s['sha256'] for s in x['ordered_source_bindings'])
             for field in ('explicit_SPLIT_source_vetoes', 'explicit_UNCERTAIN_source_holds')
             for x in human[field]})


def source_veto(bindings, bad_sources, human_pairs):
    if not bindings_valid(bindings):
        return 'UNKNOWN_source_bindings'
    if any(s['sha256'] in bad_sources for s in bindings):
        return 'REJECT_known_whole_source'
    if tuple(s['sha256'] for s in bindings) in human_pairs:
        return 'REJECT_human_ordered_source_pair'
    return None


def full_source_integrity(proof, bindings):
    """Actual combined Go SourceContext values, authenticated by artifact adapter."""
    if not isinstance(proof, dict) or proof.get('schema') != 'v85-authenticated-source-context-v1':
        return 'UNKNOWN_full_source_integrity'
    audits = proof.get('contexts')
    if not isinstance(audits, list) or len(audits) != 2:
        return 'UNKNOWN_full_source_integrity'
    for audit, expected in zip(audits, bindings):
        if not isinstance(audit, dict):
            return 'UNKNOWN_source_audit_binding'
        if audit.get('SourceSHA') != expected['sha256'] or audit.get('SourceBytes') != expected['size_bytes']:
            return 'UNKNOWN_source_audit_binding'
        if audit.get('StrictDecodePassed') is False:
            return 'REJECT_source_decode'
        digest = audit.get('OwnerPinnedProofSHA')
        if audit.get('StrictDecodePassed') is not True or not isinstance(digest, str) or len(digest) != 64 or audit.get('VerifiedProofSHA') != digest:
            return 'UNKNOWN_authenticated_strict_decode_proof'
        pts = audit.get('FramePTS')
        if not isinstance(pts, list) or len(pts) < 100 or any(not number(t, -math.inf) for t in pts):
            return 'UNKNOWN_source_decoded_pts'
        steps = [b - a for a, b in zip(pts, pts[1:])]
        if min(steps) <= 0:
            return 'REJECT_nonmonotonic_source_pts'
        median = statistics.median(steps)
        if max(steps) > max(.5, 5 * median):
            return 'REJECT_native_source_pts_gap'
        final = audit.get('LastFrameDuration'); registered = audit.get('RegisteredCaptureSpan')
        if not number(final) or final <= 0 or final > max(.5, 5 * median) or not number(registered, 20):
            return 'UNKNOWN_source_decoded_extent'
        if abs(pts[-1] + final - pts[0] - registered) > max(.2, 3 * median):
            return 'REJECT_source_capture_extent_disagreement'
    return None


def decoded_cadence_status(proof):
    # Integer spatial translations are pixels/frame; nominal FPS or requested
    # frame_seconds cannot establish native uniform temporal spacing.
    for audit in proof['contexts']:
        pts = audit['FramePTS']; steps = [b - a for a, b in zip(pts, pts[1:])]
        median = statistics.median(steps)
        if any(abs(step - median) > max(1e-6, .01 * median) for step in steps):
            return 'UNKNOWN_variable_decoded_cadence_no_PTS_normalization'
    return None


def pair_safety(pair, bindings):
    if not isinstance(pair, dict) or any(not isinstance(pair.get(s), dict) for s in ('prev', 'next')):
        return 'UNKNOWN_pair_capture_metadata'
    a, b = pair['prev'], pair['next']
    if not bindings_valid([a, b]) or source_key([a, b]) != source_key(bindings):
        return 'UNKNOWN_pair_source_binding'
    for field in ('recording_id', 'recording_job_id', 'capture_lease_token'):
        if a.get(field) is None or b.get(field) is None:
            return 'UNKNOWN_capture_identity'
        if a[field] != b[field]:
            return 'REJECT_capture_identity'
    if not isinstance(a.get('end_utc'), str) or not isinstance(b.get('start_utc'), str):
        return 'UNKNOWN_capture_boundary'
    if a['end_utc'] != b['start_utc']:
        return 'REJECT_capture_boundary_gap_or_overlap'
    sa, sb = a.get('capture_sequence'), b.get('capture_sequence')
    if type(sa) is not int or type(sb) is not int:
        return 'UNKNOWN_capture_sequence'
    if sb != sa + 1:
        return 'REJECT_capture_sequence'
    return None


def spatial_status(spatial):
    if not isinstance(spatial, dict):
        return 'UNKNOWN_spatial_absent'
    if spatial.get('version') != 'v85-spatial-translation-diagnostic-v1' or spatial.get('status') != 'diagnostic_only':
        return 'UNKNOWN_spatial_version_or_status'
    nonnegative = ('forward_heldout_residual_mad', 'backward_heldout_residual_mad',
                   'internal_heldout_residual_mad', 'direction_speed_disagreement',
                   'max_acceleration_pixels_per_frame_squared', 'texture_gradient_mad',
                   'minimum_spatial_novelty_margin', 'boundary_unwarped_mad')
    signed = ('tail_dx_pixels_per_frame', 'tail_dy_pixels_per_frame',
              'head_dx_pixels_per_frame', 'head_dy_pixels_per_frame')
    if any(not number(spatial.get(k)) for k in nonnegative) or any(
            not number(spatial.get(k), -math.inf) for k in signed):
        return 'UNKNOWN_spatial_malformed'
    if type(spatial.get('heldout_pixels')) is not int or spatial['heldout_pixels'] < PARAMETERS['minimum_heldout_pixels']:
        return 'UNKNOWN_insufficient_heldout_pixels'
    # Empty Go slices may encode null. All other missing/wrong ambiguity data abstains.
    if 'ambiguities' not in spatial or spatial['ambiguities'] not in (None, []):
        return 'UNKNOWN_spatial_ambiguity'
    if spatial.get('motion_observable') is not True:
        return 'UNKNOWN_unobservable_motion'
    tail_speed = math.hypot(spatial['tail_dx_pixels_per_frame'], spatial['tail_dy_pixels_per_frame'])
    head_speed = math.hypot(spatial['head_dx_pixels_per_frame'], spatial['head_dy_pixels_per_frame'])
    if min(tail_speed, head_speed) < .5:
        return 'UNKNOWN_unobservable_motion'
    if spatial['texture_gradient_mad'] < 2 or spatial['minimum_spatial_novelty_margin'] < 1:
        return 'UNKNOWN_texture_or_novelty'
    if spatial['direction_speed_disagreement'] >= 1 or spatial['max_acceleration_pixels_per_frame_squared'] >= 1:
        return 'REJECT_spatial_motion_disagreement'
    if max(abs(spatial[k]) for k in signed) >= 4:
        return 'UNKNOWN_search_radius_saturated'
    f = spatial['forward_heldout_residual_mad']; b = spatial['backward_heldout_residual_mad']
    interior = spatial['internal_heldout_residual_mad']
    if interior > 4:
        return 'UNKNOWN_noisy_internal_calibration'
    if max(f, b) > max(1., 2 * interior) or abs(f - b) > max(.5, .5 * interior):
        return 'REJECT_spatial_forecast_residual'
    return 'PASS_spatial_research_diagnostic'


def spatial_provenance_status(proof, bindings):
    if not isinstance(proof, dict) or proof.get('schema') != 'v85-bound-spatial-evidence-v1':
        return 'UNKNOWN_spatial_source_provenance'
    b = proof.get('source_bindings')
    if not bindings_valid(b) or source_key(b) != source_key(bindings):
        return 'UNKNOWN_spatial_source_provenance'
    if proof.get('producer_source_sha256') != PRODUCER_SHA:
        return 'UNKNOWN_spatial_producer_binding'
    if proof.get('within_side_frame_counts') != [6, 6] or any(
            proof.get(k) is not True for k in ('original_endpoint_frames_preserved', 'presentation_order_verified')):
        return 'UNKNOWN_spatial_endpoint_or_order_proof'
    if not isinstance(proof.get('diagnostic_sha256'), str):
        return 'UNKNOWN_spatial_diagnostic_hash'
    return None


def decision(row, bad_sources=(), human_pairs=()):
    bindings = row.get('source_bindings'); d = row.get('diagnostic_window20')
    out = dict(version=VERSION, baseline79_nomination=False, baselineK_nomination=False,
               spatial_nomination=False, nomination=False, spatial_status='UNKNOWN_spatial_absent',
               continuity_qualification='UNKNOWN', physical_source_clock='UNKNOWN',
               qualified_review_acceptance=False, automatic_join_authorized=False,
               production_authorized=False, review_only=True, audio_continuity='UNKNOWN',
               audio_inventory=row.get('research_audio_inventory', 'UNKNOWN'),
               video_only_research_authority='temporary_video_only_pending_audiofill')
    veto = source_veto(bindings, bad_sources, human_pairs)
    if veto:
        out['status'] = veto
        return out
    replay = row.get('research_packet_replay')
    if isinstance(replay, dict) and replay.get('original_replay_hold') is True:
        out['status'] = 'HOLD_original_packet_replay'
        return out
    try:
        hold = BASE79.integrity_hold(d)
        n79, nk = BASE79.nominate(d), K.nominate(d)
    except (KeyError, TypeError, ValueError, ZeroDivisionError, AttributeError):
        out['status'] = 'UNKNOWN_malformed_native_diagnostic'
        return out
    research_diagnostic = d
    if hold == 'explicit_audio_inventory_unqualified':
        # Preserve the pinned baselines; only the separate video-only spatial
        # research lane may proceed with explicit actual inventory retained.
        fields = [d[k] for k in ('audio_packets', 'audio_stream_count') if k in d]
        inventory = row.get('research_audio_inventory')
        if (fields and all(type(v) is int and v >= 0 for v in fields) and
                isinstance(inventory, list) and len(inventory) == 2 and
                all(isinstance(v, dict) and type(v.get('has_audio')) is bool for v in inventory)):
            research_diagnostic = {k: v for k, v in d.items() if k not in ('audio_packets', 'audio_stream_count')}
            hold = BASE79.integrity_hold(research_diagnostic)
    if hold:
        out['status'] = 'HOLD_' + hold
        return out
    out.update(baseline79_nomination=n79, baselineK_nomination=nk, nomination=nk)
    integrity = full_source_integrity(row.get('full_source_integrity'), bindings)
    cadence = decoded_cadence_status(row['full_source_integrity']) if not integrity else 'UNKNOWN_decoded_cadence'
    safety = pair_safety(row.get('pair'), bindings)
    spatial = spatial_status(row.get('spatial_temporal_diagnostic'))
    spatial_provenance = spatial_provenance_status(row.get('spatial_provenance'), bindings)
    if not spatial_provenance:
        try:
            actual_hash = hashlib.sha256(json.dumps(row.get('spatial_temporal_diagnostic'), sort_keys=True,
                                                  separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        except (ValueError, TypeError):
            actual_hash = None
        if row['spatial_provenance']['diagnostic_sha256'] != actual_hash:
            spatial_provenance = 'UNKNOWN_spatial_diagnostic_hash'
    out.update(source_integrity_status=integrity or 'PASS_source_local_integrity',
               pair_safety_status=safety or 'PASS_capture_metadata_only', spatial_status=spatial,
               spatial_provenance_status=spatial_provenance or 'PASS_bound_spatial_diagnostic',
               decoded_cadence_status=cadence or 'PASS_native_decoded_uniform_cadence')
    # No spatial override of an estimated overlap: exact payload ordering alone
    # is not a qualified cut or physical-clock proof.
    if d.get('reason') == 'frame_overlap':
        out['spatial_status'] = 'UNKNOWN_overlap_cut_and_physical_continuity'
    elif not integrity and not cadence and not safety and not spatial_provenance and spatial == 'PASS_spatial_research_diagnostic':
        out['spatial_nomination'] = True
        out['nomination'] = True
    if integrity and integrity.startswith('REJECT_') or safety and safety.startswith('REJECT_'):
        out.update(nomination=False, spatial_nomination=False)
        out['status'] = integrity if integrity and integrity.startswith('REJECT_') else safety
    else:
        out['status'] = 'RESEARCH_nomination' if out['nomination'] else 'RESEARCH_not_nominated'
    return out


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit('usage: classifier85.py BAD_SOURCE_INVENTORY HUMAN_ORDERED_VETO < rows.jsonl')
    bad, human = load_vetoes(sys.argv[1], sys.argv[2])
    for line in sys.stdin:
        if line.strip():
            print(json.dumps(decision(json.loads(line), bad, human), allow_nan=False))
