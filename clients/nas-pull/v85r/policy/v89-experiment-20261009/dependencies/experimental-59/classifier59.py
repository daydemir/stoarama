"""Aggressive source-bound feature frontier for human failure-boundary review.
Retains frozen49. Interior matcher minima are evidence, never temporal cuts.
No media writes, automatic stitching, production authorization or FP guarantee.
"""
import pathlib,hashlib,importlib.util,math
VERSION='model59-two-context-failure-frontier-review-20261009'
BASE49_SHA256='8d453c6abe4f7560ecea55b9a2cbf3044340591273e16bf5063d22070b6d050a'
_path=pathlib.Path(__file__).resolve().parent.parent/'experimental-49/classifier49.py'
if hashlib.sha256(_path.read_bytes()).hexdigest()!=BASE49_SHA256:raise RuntimeError('Frozen49 implementation differs')
_spec=importlib.util.spec_from_file_location('frozen_model49',_path);_base49=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_base49)
PARAMETERS=__import__('json').loads('{"review_only":true,"branches":[{"tail":10000,"head":10000,"boundary":0.75,"tail_sharp":0.95,"head_sharp":0.99,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_01"},{"tail":10000,"head":2,"boundary":12,"tail_sharp":1.0,"head_sharp":0.75,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_02"},{"tail":10000,"head":10000,"boundary":12,"tail_sharp":0.95,"head_sharp":0.75,"motion":3,"gop":["complete","irregular"],"name":"numeric_context_frontier_03"},{"tail":10000,"head":10000,"boundary":12,"tail_sharp":0.95,"head_sharp":0.99,"motion":6,"gop":["complete","irregular"],"name":"numeric_context_frontier_04"},{"tail":10000,"head":0,"boundary":2,"tail_sharp":0.99,"head_sharp":0.99,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_05"},{"tail":0,"head":10000,"boundary":8,"tail_sharp":0.95,"head_sharp":0.9,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_06"},{"tail":10000,"head":2,"boundary":1.4,"tail_sharp":0.5,"head_sharp":0.95,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_07"},{"tail":10,"head":10000,"boundary":8,"tail_sharp":0.95,"head_sharp":0.99,"motion":3,"gop":["complete","irregular"],"name":"numeric_context_frontier_08"},{"tail":10000,"head":2,"boundary":1.4,"tail_sharp":0.95,"head_sharp":0.95,"motion":0.8,"gop":["complete","irregular"],"name":"numeric_context_frontier_09"},{"tail":1,"head":10000,"boundary":1.4,"tail_sharp":0.95,"head_sharp":0.95,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_10"},{"tail":10000,"head":10000,"boundary":2,"tail_sharp":0.95,"head_sharp":0.95,"motion":3,"gop":["complete","irregular"],"name":"numeric_context_frontier_11"},{"tail":0,"head":10000,"boundary":2,"tail_sharp":0.75,"head_sharp":0.95,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_12"},{"tail":0,"head":0,"boundary":8,"tail_sharp":0.75,"head_sharp":0.95,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_13"},{"tail":10000,"head":1,"boundary":2,"tail_sharp":0.825,"head_sharp":0.825,"motion":0.8,"gop":["complete","irregular"],"name":"numeric_context_frontier_14"},{"tail":1,"head":10000,"boundary":4,"tail_sharp":0.5,"head_sharp":0.95,"motion":0.05,"gop":["complete","irregular"],"name":"numeric_context_frontier_15"},{"tail":1,"head":10000,"boundary":12,"tail_sharp":0.9,"head_sharp":0.9,"motion":1.5,"gop":["complete","irregular"],"name":"numeric_context_frontier_16"}],"clock_qualification_sha256":null,"automatic_production_authorized":false}')
def valid_native_offsets(d):
 m=d.get('frame_match',{})
 fields=['window_seconds','tail_median_mad','head_median_mad','tail_min_mad','head_min_mad','boundary_mad','key_step_median_mad']
 if any(type(m.get(k))not in(int,float)or not math.isfinite(m[k])or m[k]<0 for k in fields):return False
 if type(d.get('db_gap_seconds'))not in(int,float):return False
 for side in ['tail','head']:
  count=m.get(side+'_frames');offset=m.get(side+'_min_offset_frames')
  if type(count)is not int or type(offset)is not int or count<=0 or not 0<=offset<count:return False
 period=d.get('frame_seconds')
 return type(period)in(int,float)and math.isfinite(period)and .01<=period<=.2

def visual_branches(d,p=PARAMETERS):
 if not valid_native_offsets(d):return []
 if _base49.nominate(d):return ['retained49']
 return [b['name']for b in p['branches']if _base49._base39._base._core(d,b)]
def nominate(d,p=PARAMETERS,pair=None,clock_evidence=None,clock_qualification=None):
 # Clock qualification deliberately unpinned. Caller booleans cannot enable it.
 return bool(visual_branches(d,p))
def decision(d,pair=None,human_split_bindings=(),clock_evidence=None,clock_qualification=None):
 key=tuple(pair[s]['sha256']for s in ['prev','next'])if pair else None
 held=key in {tuple(x)for x in human_split_bindings};visual=visual_branches(d)
 return {'version':VERSION,'decision':'held_human_split'if held else 'join_candidate'if visual else 'not_nominated','visual_branches':visual,'human_split_hold':held,'clock':{'status':'unknown','reason':'clock_qualification_not_pinned','nomination':False},'review_only':True,'automatic_join_authorized':False}
