"""Unpublished ordinary-motion contextual frontier; no alias, production or cut.
This exploratory source feature policy is separate from frozen model59.
"""
import pathlib,hashlib,importlib.util,math,json
VERSION='model59-revision-b-ordinary-motion-context-frontier-review-20261009'
BASE59_SHA256='3d1d1a05740c57030c0374204a6bfd335a375445dac108b5ee9a43c390118abb'
p=pathlib.Path(__file__).resolve().parent.parent/'classifier59.py'
if hashlib.sha256(p.read_bytes()).hexdigest()!=BASE59_SHA256:raise RuntimeError('Frozen59 differs')
s=importlib.util.spec_from_file_location('frozen59',p);_base59=importlib.util.module_from_spec(s);s.loader.exec_module(_base59)
PARAMETERS=json.loads('{"review_only":true,"automatic_join_authorized":false,"clock_qualification_sha256":null,"branches":[{"tail":10000,"head":10000,"boundary":6,"tail_sharp":0.5,"head_sharp":0.95,"motion":0.05,"gop":["complete","irregular"],"name":"ordinary_motion_context_01"},{"tail":10000,"head":10000,"boundary":6,"tail_sharp":0.99,"head_sharp":0.75,"motion":0.05,"gop":["complete","irregular"],"name":"ordinary_motion_context_02"},{"tail":10000,"head":10000,"boundary":2,"tail_sharp":0.9,"head_sharp":0.99,"motion":0.05,"gop":["complete","irregular"],"name":"ordinary_motion_context_03"},{"tail":10000,"head":10000,"boundary":3,"tail_sharp":0.95,"head_sharp":0.9,"motion":0.8,"gop":["complete","irregular"],"name":"ordinary_motion_context_04"},{"tail":10,"head":10000,"boundary":6,"tail_sharp":0.9,"head_sharp":0.9,"motion":0.05,"gop":["complete","irregular"],"name":"ordinary_motion_context_05"},{"tail":10000,"head":60,"boundary":6,"tail_sharp":0.99,"head_sharp":1.0,"motion":6,"gop":["complete","irregular"],"name":"ordinary_motion_context_06"},{"tail":10000,"head":60,"boundary":1.4,"tail_sharp":0.95,"head_sharp":0.99,"motion":0.05,"gop":["complete","irregular"],"name":"ordinary_motion_context_07"},{"tail":10000,"head":10000,"boundary":3,"tail_sharp":0.95,"head_sharp":0.99,"motion":3,"gop":["complete","irregular"],"name":"ordinary_motion_context_08"},{"tail":60,"head":10000,"boundary":1.4,"tail_sharp":0.95,"head_sharp":0.95,"motion":0.05,"gop":["complete","irregular"],"name":"ordinary_motion_context_09"},{"tail":30,"head":10000,"boundary":3,"tail_sharp":0.75,"head_sharp":0.825,"motion":0.05,"gop":["complete","irregular"],"name":"ordinary_motion_context_10"}]}')
def ordinary_core(d,p):
 if not _base59.valid_native_offsets(d):return False
 if d.get('reason')not in {'continuous','keyframe_boundary_continuous','source_adjacent','frame_jump','frame_boundary_ambiguous','frame_low_motion'}or d['db_gap_seconds']!=0 or d.get('gop_lattice',{}).get('status')not in p['gop']or d.get('source_evidence',{}).get('status')not in {'adjacent','unavailable'}:return False
 m=d['frame_match'];step=m.get('step_p95_mad')
 if type(step)not in(int,float)or not math.isfinite(step)or step<=0:return False
 return m['window_seconds']==20 and min(m['tail_frames'],m['head_frames'])>=100 and m['tail_min_offset_frames']<=p['tail']and m['head_min_offset_frames']<=p['head']and min(m['tail_median_mad'],m['head_median_mad'])>=p['motion']and m['tail_min_mad']<=p['tail_sharp']*m['tail_median_mad']and m['head_min_mad']<=p['head_sharp']*m['head_median_mad']and m['boundary_mad']<=p['boundary']*step

def visual_branches(d):
 if _base59.nominate(d):return ['retained_frozen59']
 return [b['name']for b in PARAMETERS['branches']if ordinary_core(d,b)]
def nominate(d):return bool(visual_branches(d))
