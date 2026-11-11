"""V89 experimental review nominations. Preserve frozen V79 integrity guards.
This classifier does not authorize production joins or certify temporal cuts.
"""
import pathlib,hashlib,importlib.util,math
VERSION='model89-calibrated-activity-bands-revision-j-review-20261010'
BASE79_SHA256='d79c05e1c13f502ef09feffbc2324cf07596b438f5d4622bd5b4bf262445ced6'
p=pathlib.Path(__file__).resolve().parent.parent.parent/'experimental-79/classifier79.py'
if hashlib.sha256(p.read_bytes()).hexdigest()!=BASE79_SHA256:raise RuntimeError('Frozen79 differs')
s=importlib.util.spec_from_file_location('frozen79',p);_base79=importlib.util.module_from_spec(s);s.loader.exec_module(_base79)
PARAMETERS={'review_only': True, 'automatic_join_authorized': False, 'target_review_nomination_rate': 0.99, 'clock_qualification_sha256': None, 'trim_qualification_sha256': None, 'branches': [{'name': 'calibrated_activity_band_1', 'motion': 0.75, 'motion_max': 2, 'ordinary_boundary': 48, 'key_boundary': 1.4, 'tail_sharp': 1, 'head_sharp': 1}, {'name': 'calibrated_activity_band_2', 'motion': 32, 'motion_max': 10000, 'ordinary_boundary': 24, 'key_boundary': 24, 'tail_sharp': 0.75, 'head_sharp': 0.75}, {'name': 'calibrated_activity_band_3', 'motion': 2.5, 'motion_max': 3, 'ordinary_boundary': 24, 'key_boundary': 1.4, 'tail_sharp': 0.5, 'head_sharp': 0.5}, {'name': 'calibrated_activity_band_4', 'motion': 6, 'motion_max': 24, 'ordinary_boundary': 24, 'key_boundary': 24, 'tail_sharp': 0.95, 'head_sharp': 0.95}, {'name': 'calibrated_activity_band_5', 'motion': 4, 'motion_max': 6, 'ordinary_boundary': 12, 'key_boundary': 8, 'tail_sharp': 0.85, 'head_sharp': 0.85}, {'name': 'calibrated_activity_band_6', 'motion': 0.65, 'motion_max': 0.75, 'ordinary_boundary': 12, 'key_boundary': 1.4, 'tail_sharp': 0.83, 'head_sharp': 0.83}]}
PARAMETERS['branches'].append({'name':'calibrated_very_high_activity_band_7','motion':80,'motion_max':10000,'ordinary_boundary':26,'key_boundary':26,'tail_sharp':.75,'head_sharp':.9})
PARAMETERS['branches'].extend([{'name':'calibrated_mid_activity_sharp_context_band_8','motion':3,'motion_max':4,'ordinary_boundary':24,'key_boundary':24,'tail_sharp':.5,'head_sharp':.75},{'name':'calibrated_high_activity_sharp_context_band_9','motion':64,'motion_max':80,'ordinary_boundary':24,'key_boundary':26,'tail_sharp':.75,'head_sharp':.85}])
PARAMETERS['branches'].append({'name':'calibrated_low_activity_sharp_context_band_10','motion':.5,'motion_max':.65,'ordinary_boundary':12,'key_boundary':1.4,'tail_sharp':.7,'head_sharp':.7})
PARAMETERS['branches'].extend([{'name':'calibrated_low_activity_context_band_11','motion':.65,'motion_max':.75,'ordinary_boundary':26,'key_boundary':1.4,'tail_sharp':.85,'head_sharp':.86},{'name':'calibrated_lower_activity_context_band_12','motion':.4,'motion_max':.65,'ordinary_boundary':26,'key_boundary':1.4,'tail_sharp':.7,'head_sharp':.76}])
PARAMETERS['bounded_context_shape']={'motion_min':.05,'motion_max':3,'motion_balance_max':1.2,'edge_motion_fraction_max':.75,'ordinary_boundary':12,'key_boundary':1.4,'tail_sharp':.55,'head_sharp':.55,'offset_min':1,'offset_max_min':5,'offset_max_max':20,'offset_seconds_max':1}
PARAMETERS['balanced_motion_shape']={'motion_min':2.5,'motion_max':3,'motion_balance_max':1.2,'edge_motion_fraction_max':1.1,'ordinary_boundary':6,'key_boundary':2.6,'tail_sharp':.95,'head_sharp':.95}
PARAMETERS['relative_motion_band']={'boundary_min_motion_ratio_min':.5,'boundary_min_motion_ratio_max':1.1,'boundary_max_motion_ratio_max':.9,'key_boundary':1.25}
def additional_branches(d):
 if _base79.integrity_hold(d):return []
 m=d['frame_match'];step=m.get('step_p95_mad')
 if type(step)not in(int,float)or not math.isfinite(step)or step<=0:return []
 if m['window_seconds']!=20 or min(m['tail_frames'],m['head_frames'])<100 or m['key_step_median_mad']<=0:return []
 out=[b['name']for b in PARAMETERS['branches']if b['motion']<=min(m['tail_median_mad'],m['head_median_mad'])<=b['motion_max']and m['tail_min_mad']<=b['tail_sharp']*m['tail_median_mad']and m['head_min_mad']<=b['head_sharp']*m['head_median_mad']and m['boundary_mad']<=b['ordinary_boundary']*step and m['boundary_mad']<=b['key_boundary']*m['key_step_median_mad']+.05]
 b=PARAMETERS['bounded_context_shape'];lo=min(m['tail_median_mad'],m['head_median_mad']);offsets=[m['tail_min_offset_frames'],m['head_min_offset_frames']]
 if (b['motion_min']<=lo<=b['motion_max'] and max(m['tail_median_mad'],m['head_median_mad'])<=b['motion_balance_max']*lo and m['boundary_mad']<=b['edge_motion_fraction_max']*lo and m['boundary_mad']<=b['ordinary_boundary']*step and m['boundary_mad']<=b['key_boundary']*m['key_step_median_mad']+.05 and m['tail_min_mad']<=b['tail_sharp']*m['tail_median_mad'] and m['head_min_mad']<=b['head_sharp']*m['head_median_mad'] and min(offsets)>=b['offset_min'] and b['offset_max_min']<=max(offsets)<=b['offset_max_max'] and max(offsets)*d['frame_seconds']<=b['offset_seconds_max']):out.append('bounded_bidirectional_context_shape')
 b=PARAMETERS['balanced_motion_shape']
 if (b['motion_min']<=lo<=b['motion_max'] and max(m['tail_median_mad'],m['head_median_mad'])<=b['motion_balance_max']*lo and m['boundary_mad']<=b['edge_motion_fraction_max']*lo and m['boundary_mad']<=b['ordinary_boundary']*step and m['boundary_mad']<=b['key_boundary']*m['key_step_median_mad']+.05 and m['tail_min_mad']<=b['tail_sharp']*m['tail_median_mad'] and m['head_min_mad']<=b['head_sharp']*m['head_median_mad']):out.append('balanced_bidirectional_motion_shape')
 b=PARAMETERS['relative_motion_band']
 if (lo>0 and b['boundary_min_motion_ratio_min']*lo<=m['boundary_mad']<=b['boundary_min_motion_ratio_max']*lo and m['boundary_mad']<=b['boundary_max_motion_ratio_max']*max(m['tail_median_mad'],m['head_median_mad']) and m['boundary_mad']<=b['key_boundary']*m['key_step_median_mad']+.05):out.append('relative_motion_keyframe_band')
 return out
def visual_branches(d):
 if _base79.integrity_hold(d):return []
 if _base79.nominate(d):return ['retained79']
 return additional_branches(d)
def nominate(d):return bool(visual_branches(d))
def decision(d,pair=None,native=None,human_split_bindings=(),trim_evidence=None,trim_qualification=None):
 hold=_base79.integrity_hold(d)or _base79._base69.source_binding_hold(d,pair,native)
 if pair and tuple(pair[x]['sha256']for x in('prev','next'))in {tuple(x)for x in human_split_bindings}:hold='explicit_human_split_source_hold'
 visual=visual_branches(d);kind='estimated_overlap_review_nomination'if d.get('reason')=='frame_overlap'and visual else'straight_review_nomination'if visual else'not_nominated'
 return {'version':VERSION,'decision':'held_'+hold if hold else kind,'nomination':bool(visual)and hold is None,'original_reason':d.get('reason'),'visual_branches':visual,'continuity_qualification':'UNKNOWN','trim':_base79._base69.trim_decision(d,pair,trim_evidence,trim_qualification),'clock_qualification':None,'review_only':True,'automatic_join_authorized':False,'production_authorized':False}
