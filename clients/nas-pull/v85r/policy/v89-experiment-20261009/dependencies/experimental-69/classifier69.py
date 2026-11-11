"""Experimental69 review nominations, not certified joins or temporal cuts.
Estimated visual overlaps remain unqualified hypotheses with original evidence.
Known packet replay/source contradiction/native failures are never reconsidered.
"""
import pathlib,hashlib,importlib.util,math,json
VERSION='model69-unverified-overlap-reconsideration-dual-calibration-review-20261009'
BASE59B_SHA256='0edfb031077a5e4620f0b793e4420fda20ab067a354aae8e864f5af5f20126cf'
p=pathlib.Path(__file__).resolve().parent.parent/'experimental-59/revision-b/classifier59_revision_b.py'
if hashlib.sha256(p.read_bytes()).hexdigest()!=BASE59B_SHA256:raise RuntimeError('Frozen59B differs')
s=importlib.util.spec_from_file_location('frozen59b',p);_base59b=importlib.util.module_from_spec(s);s.loader.exec_module(_base59b)
PARAMETERS=json.loads('{"review_only":true,"automatic_join_authorized":false,"unanchored_seconds":0.5,"ordinary_step_guard":6,"key_step_guard":1.4,"overlap_min_activity_mad":0.8,"clock_qualification_sha256":null,"trim_qualification_sha256":null,"straight_branches":[{"tail":10000,"head":10000,"tail_sharp":0.875,"head_sharp":0.95,"motion":0.8,"boundary":0.75,"gop":["complete","irregular"],"calibration":"key_plus_ordinary","name":"additional_context_calibration_01"},{"tail":10000,"head":60,"tail_sharp":0.8,"head_sharp":0.5,"motion":0.01,"boundary":0.2,"gop":["complete","irregular"],"calibration":"context_geometric","name":"additional_context_calibration_02"}]}')
def native_timeline_hold(d):
 # Generic denying guard only. Missing timeline proof stays UNKNOWN and never becomes production-qualified.
 evidence=d.get('native_timeline_evidence',[])
 return any(type(e.get('max_source_pts_step_seconds'))in(int,float)and type(e.get('median_source_pts_step_seconds'))in(int,float)and e['median_source_pts_step_seconds']>0 and e['max_source_pts_step_seconds']>max(.5,5*e['median_source_pts_step_seconds'])for e in evidence if isinstance(e,dict))
def unanchored_high_boundary(d,p=PARAMETERS):
 if not _base59b._base59.valid_native_offsets(d):return True
 if d.get('source_evidence',{}).get('status')=='adjacent':return False
 m=d['frame_match'];step=m.get('step_p95_mad')
 return min(m['tail_min_offset_frames'],m['head_min_offset_frames'])*d['frame_seconds']>p['unanchored_seconds']and(type(step)not in(int,float)or not math.isfinite(step)or step<=0 or(m['boundary_mad']>p['ordinary_step_guard']*step and m['boundary_mad']>p['key_step_guard']*m['key_step_median_mad']+.05))
def additional_core(d,b):
 m=d.get('frame_match',{})
 if b['calibration']=='key_plus_ordinary':k=m['key_step_median_mad']+m.get('step_p95_mad',0)
 else:k=math.sqrt(m['tail_median_mad']*m['head_median_mad'])
 # Compare against actual source features, preserving the original frozen evidence.
 transformed=dict(d,frame_match=dict(m,key_step_median_mad=k,boundary_mad=m['boundary_mad']+.05))
 return _base59b._base59._base49._base39._base._core(transformed,b)
def straight_branches(d):
 if native_timeline_hold(d)or unanchored_high_boundary(d):return []
 if _base59b.nominate(d):return ['retained59b_after_chronology_guard']
 return [b['name']for b in PARAMETERS['straight_branches']if additional_core(d,b)]
def estimated_overlap_branches(d):
 if d.get('reason')!='frame_overlap'or d.get('gop_lattice',{}).get('status')!='complete'or native_timeline_hold(d)or unanchored_high_boundary(d):return []
 m=d['frame_match']
 if min(m['tail_median_mad'],m['head_median_mad'])<PARAMETERS['overlap_min_activity_mad']:return []
 # Scalar match verdict is a similarity estimate. This counterfactual only selects human review.
 candidate=dict(d,reason='frame_boundary_ambiguous')
 if _base59b.nominate(candidate):return ['estimated_overlap_reconsidered_by59b_features']
 return [b['name']for b in PARAMETERS['straight_branches']if additional_core(candidate,b)]
def visual_branches(d):return straight_branches(d)+estimated_overlap_branches(d)
def nominate(d):return bool(visual_branches(d))
def source_binding_hold(d,pair,native):
 if pair is None or native is None:return None
 expected=[{k:pair[s][k]for k in ['clip_id','sha256','size_bytes']}for s in ['prev','next']]
 if native.get('source_bindings')!=expected or native.get('source_sha256_verified')is not True or native.get('adjacent_output_endpoints_verified')is not True:return 'missing_or_mismatched_native_source_binding'
 if d.get('prev_clip_id')!=pair['prev']['clip_id']or d.get('next_clip_id')!=pair['next']['clip_id']:return 'native_clip_identity_mismatch'
 a,b=pair['prev'],pair['next']
 if a.get('recording_id')!=b.get('recording_id')or a.get('recording_job_id')!=b.get('recording_job_id')or a.get('capture_lease_token')!=b.get('capture_lease_token')or a.get('end_utc')!=b.get('start_utc'):return 'capture_identity_or_database_boundary_hold'
 seq=[a.get('capture_sequence'),b.get('capture_sequence')]
 if seq!=[None,None]and(type(seq[0])is not int or type(seq[1])is not int or seq[1]!=seq[0]+1):return 'capture_sequence_conflict'
 return None
def trim_decision(d,pair=None,evidence=None,qualification=None):
 # No qualified native moving-sequence/cut receipt exists in this experiment.
 return {'status':'UNKNOWN','qualification_sha256':None,'verified_join_with_trim':False,'trim_frames':None,'reason':'source_bound_packet_or_moving_sequence_cut_qualification_missing'}
def decision(d,pair=None,native=None,human_split_bindings=(),trim_evidence=None,trim_qualification=None):
 visual=visual_branches(d);held=source_binding_hold(d,pair,native)
 key=tuple(pair[s]['sha256']for s in ['prev','next'])if pair else None
 if key in {tuple(x)for x in human_split_bindings}:held='explicit_human_split_source_hold'
 kind='estimated_overlap_review_nomination'if estimated_overlap_branches(d)else 'straight_review_nomination'if straight_branches(d)else 'not_nominated'
 return {'version':VERSION,'decision':'held_'+held if held else kind,'nomination':bool(visual)and held is None,'original_reason':d.get('reason'),'visual_branches':visual,'continuity_qualification':'UNKNOWN','native_timeline_proof':'UNKNOWN_unless_separately_verified','trim':trim_decision(d,pair,trim_evidence,trim_qualification),'review_only':True,'automatic_join_authorized':False,'production_authorized':False,'clock_qualification':None}
