"""Immutable experimental V7.99 head-and-boundary with conditional one-sided tail review fork; nominations for visual audit only.
No media writes, cuts, production registration or automatic promotion.
"""
import json,math,sys
VERSION='v7p99-one-sided-tail3-review-20261009'
PARAMETERS=dict(tail=1,head=60,boundary=12,tail_sharp=.9,head_sharp=.875,motion=.4,gop=['complete','irregular'],conditional_tail=dict(tail=3,head=1,boundary=8,tail_sharp=.75,head_sharp=.8,motion=.8,gop=['complete','irregular']))
def _core(d, p):
    if d.get('reason') not in {'continuous', 'keyframe_boundary_continuous', 'frame_jump', 'frame_boundary_ambiguous', 'frame_low_motion', 'source_adjacent'}:
        return False
    if d.get('db_gap_seconds', 1) != 0 or d.get('gop_lattice', {}).get('status') not in p.get('gop', ['complete']) or d.get('source_evidence', {}).get('status') not in {'adjacent', 'unavailable'}:
        return False
    m = d.get('frame_match', {})
    keys = ['window_seconds', 'tail_frames', 'head_frames', 'tail_min_offset_frames', 'head_min_offset_frames', 'tail_median_mad', 'head_median_mad', 'tail_min_mad', 'head_min_mad', 'boundary_mad', 'key_step_median_mad']
    if any((k not in m or not isinstance(m[k], (int, float)) or (not math.isfinite(m[k])) or (m[k] < 0) for k in keys)):
        return False
    return m['window_seconds'] == 20 and min(m['tail_frames'], m['head_frames']) >= 100 and (m['tail_min_offset_frames'] <= p['tail']) and (m['head_min_offset_frames'] <= p['head']) and (min(m['tail_median_mad'], m['head_median_mad']) >= p['motion']) and (m['tail_min_mad'] <= p['tail_sharp'] * m['tail_median_mad']) and (m['head_min_mad'] <= p['head_sharp'] * m['head_median_mad']) and (m['key_step_median_mad'] > 0) and (m['boundary_mad'] <= p['boundary'] * m['key_step_median_mad'] + 0.05)
def nominate(d, p):
    return _core(d, p) or (isinstance(p.get('conditional_tail'), dict) and _core(d, p['conditional_tail']))
if __name__=='__main__':
 import hashlib,pathlib
 sha=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
 for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
  x=json.loads(line);d=x.get('diagnostic_window20',x.get('native',{}).get('diagnostic_window20',x.get('current_v3',{})))
  print(json.dumps(dict(id=x.get('id',x.get('pair',{}).get('id')),experimental_version=VERSION,implementation_sha256=sha,parameters=PARAMETERS,decision='join_candidate' if nominate(d,PARAMETERS) else 'split',review_only=True,automatic_join_authorized=False,diagnostic_window20=d)))
