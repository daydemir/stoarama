"""Experimental79 review frontier on frozen69. No continuity/cut certification.
General source-feature predicates only; source hashes govern separate human vetoes.
"""
import pathlib,hashlib,importlib.util,math
VERSION='model79-ordinary-keyframe-calibrated-motion-frontier-review-20261009'
BASE69_SHA256='7c56d598d5a4595081a56d96669611b3c6de6f364c5426f2640d1b62f261d2e3'
p=pathlib.Path(__file__).resolve().parent.parent/'experimental-69/classifier69.py'
if hashlib.sha256(p.read_bytes()).hexdigest()!=BASE69_SHA256:raise RuntimeError('Frozen69 differs')
s=importlib.util.spec_from_file_location('frozen69',p);_base69=importlib.util.module_from_spec(s);s.loader.exec_module(_base69)
PARAMETERS={'review_only':True,'automatic_join_authorized':False,'target_review_nomination_rate':.99,'clock_qualification_sha256':None,'trim_qualification_sha256':None,'branches':[
 {'name':'normal_activity_dual_source_calibration','motion':.8,'ordinary_boundary':6.,'key_boundary':2.,'tail_sharp':1.,'head_sharp':1.},
 {'name':'strong_activity_bidirectional_sharp_context','motion':3.,'ordinary_boundary':24.,'key_boundary':12.,'tail_sharp':.75,'head_sharp':.75},
 {'name':'low_activity_sharp_tail_context','motion':.05,'ordinary_boundary':6.,'key_boundary':8.,'tail_sharp':.5,'head_sharp':1.}]}
REASONS={'continuous','keyframe_boundary_continuous','source_adjacent','frame_jump','frame_boundary_ambiguous','frame_low_motion','frame_overlap'}
def integrity_hold(d):
 if not _base69._base59b._base59.valid_native_offsets(d):return 'malformed_native_features'
 if d['db_gap_seconds']!=0:return 'capture_boundary_gap'
 if d.get('reason')not in REASONS:return 'native_replay_decode_or_source_failure'
 if d.get('gop_lattice',{}).get('status')not in('complete','irregular'):return 'truncated_or_unknown_gop'
 if d.get('source_evidence',{}).get('status')not in('adjacent','unavailable'):return 'source_timeline_contradiction'
 if _base69.native_timeline_hold(d):return 'native_source_pts_gap'
 if _base69.unanchored_high_boundary(d):return 'unanchored_high_boundary'
 for key in('audio_packets','audio_stream_count'):
  if key in d and(type(d[key])is not int or d[key]!=0):return 'explicit_audio_inventory_unqualified'
 if d.get('verified_packet_replay')is True:return 'verified_packet_replay'
 for key in('packet_payload_chain_matches','source_decode_ok'):
  if key in d and d[key]is not True:return 'explicit_native_integrity_failure'
 for evidence in d.get('native_timeline_evidence',[]):
  if not isinstance(evidence,dict):return 'malformed_native_timeline'
  for key in('max_source_pts_step_seconds','median_source_pts_step_seconds'):
   if key in evidence and(type(evidence[key])not in(int,float)or not math.isfinite(evidence[key])or evidence[key]<=0):return 'malformed_native_timeline'
 return None
def additional_branches(d):
 if integrity_hold(d):return []
 m=d['frame_match'];step=m.get('step_p95_mad')
 if type(step)not in(int,float)or not math.isfinite(step)or step<=0:return []
 if m['window_seconds']!=20 or min(m['tail_frames'],m['head_frames'])<100:return []
 return [b['name']for b in PARAMETERS['branches']if min(m['tail_median_mad'],m['head_median_mad'])>=b['motion']and m['tail_min_mad']<=b['tail_sharp']*m['tail_median_mad']and m['head_min_mad']<=b['head_sharp']*m['head_median_mad']and m['boundary_mad']<=b['ordinary_boundary']*step and m['boundary_mad']<=b['key_boundary']*m['key_step_median_mad']+.05]
def visual_branches(d):
 if integrity_hold(d):return []
 if _base69.nominate(d):return ['retained69_after_native_integrity_guard']
 return additional_branches(d)
def nominate(d):return bool(visual_branches(d))
def decision(d,pair=None,native=None,human_split_bindings=(),trim_evidence=None,trim_qualification=None):
 hold=integrity_hold(d)or _base69.source_binding_hold(d,pair,native)
 if pair and tuple(pair[x]['sha256']for x in('prev','next'))in {tuple(x)for x in human_split_bindings}:hold='explicit_human_split_source_hold'
 visual=visual_branches(d);kind='estimated_overlap_review_nomination'if d.get('reason')=='frame_overlap'and visual else'straight_review_nomination'if visual else'not_nominated'
 return {'version':VERSION,'decision':'held_'+hold if hold else kind,'nomination':bool(visual)and hold is None,'original_reason':d.get('reason'),'visual_branches':visual,'continuity_qualification':'UNKNOWN','trim':_base69.trim_decision(d,pair,trim_evidence,trim_qualification),'clock_qualification':None,'review_only':True,'automatic_join_authorized':False,'production_authorized':False}
