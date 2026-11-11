"""Independent cached-feature research candidate; no production or review admission."""
import pathlib,hashlib,importlib.util,math
VERSION='model89-relative-continuity-envelope-revision-k-research-20261010'
p=pathlib.Path(__file__).parent.parent/'revision-j/classifier89_revision_j.py'
assert hashlib.sha256(p.read_bytes()).hexdigest()=='b1aa7c3ab9738fb1e4fe72b2066f19eddc748fde38456b199cca524f838ddc3c'
s=importlib.util.spec_from_file_location('pinned89j',p);j=importlib.util.module_from_spec(s);s.loader.exec_module(j)
_base79=j._base79
BASE79_SHA256=j.BASE79_SHA256
PARAMETERS={'relative_boundary_min':.25,'relative_boundary_max':1.5,'boundary_high_motion_max':1,'boundary_key_max':1.25,'boundary_p95_max':32,'motion_balance_max':2,'sharpness_max':.9,'motion_min':.05,'review_only':True}
def extra(d):
 if _base79.integrity_hold(d):return False
 m=d['frame_match'];step=m.get('step_p95_mad')
 if type(step)not in(int,float)or not math.isfinite(step)or step<=0:return False
 if m['window_seconds']!=20 or min(m['tail_frames'],m['head_frames'])<100:return False
 lo=min(m['tail_median_mad'],m['head_median_mad']);hi=max(m['tail_median_mad'],m['head_median_mad'])
 if lo<.05 or m['key_step_median_mad']<=0:return False
 return .25*lo<=m['boundary_mad']<=1.5*lo and m['boundary_mad']<=hi and m['boundary_mad']<=1.25*m['key_step_median_mad']+.05 and m['boundary_mad']<=32*step and hi<=2*lo and m['tail_min_mad']<=.9*m['tail_median_mad'] and m['head_min_mad']<=.9*m['head_median_mad']
def nominate(d):return j.nominate(d)or extra(d)
def decision(d):
 x=j.decision(d);x['version']=VERSION;x['nomination']=nominate(d)and not _base79.integrity_hold(d)
 if extra(d)and not j.nominate(d):x['decision']='straight_review_nomination';x['visual_branches'].append('relative_continuity_envelope')
 return x
