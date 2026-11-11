"""Model49 experimental nomination frontier, retaining frozen39.
No cuts, media writes, production approval or false-positive guarantee.
The 20s interior minimum is feature evidence, not a seam location.
Source-hash human Split governance is an independent hold, not an ID predicate.
"""
import pathlib,hashlib,importlib.util,math
VERSION='model49-long-context-edge-frontier-review-20261009'
BASE39_SHA256='10bf837b8714d4eab6d7e2c7d60ef21e09983a76fe3aecd3af248dfc68f2c166'
PARAMETERS={'review_only':True,'branches':[
 {'name':'tail_context_bounded_boundary','side':'tail','context_seconds':20.0,'edge_frames':2,'boundary':1.4,'tail_sharp':.9,'head_sharp':.95,'motion':.8,'gop':['complete']},
 {'name':'tail_context_sharp_head','side':'tail','context_seconds':20.0,'edge_frames':2,'boundary':2.0,'tail_sharp':.9,'head_sharp':.825,'motion':.8,'gop':['complete']},
 {'name':'head_context_edge_anchor','side':'head','context_seconds':20.0,'edge_frames':1,'boundary':4.0,'tail_sharp':.9,'head_sharp':.825,'motion':.4,'gop':['complete']}],
 'clock_max_positive_gap_seconds':1.0,'clock_max_replay_seconds':.08,
 'clock_automatic_production_authorized':False,'clock_qualification_sha256':None}
_path=pathlib.Path(__file__).resolve().parent.parent/'experimental-punta-v7999/v7p999_one_sided_tail_frontier_review.py'
if hashlib.sha256(_path.read_bytes()).hexdigest()!=BASE39_SHA256:raise RuntimeError('Frozen39 implementation differs')
_spec=importlib.util.spec_from_file_location('frozen_model39',_path);_base39=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_base39)
def visual_branches(d,p=PARAMETERS):
 m=d.get('frame_match',{})
 for side in ['tail','head']:
  count=m.get(side+'_frames');offset=m.get(side+'_min_offset_frames')
  if type(count)is not int or type(offset)is not int or count<=0 or not 0<=offset<count:return []
 period=d.get('frame_seconds')
 if type(period)not in(int,float)or not math.isfinite(period)or not .01<=period<=.2:return []
 if _base39.nominate(d):return ['retained39']
 branches=[]
 for branch in p['branches']:
  context_frames=math.floor(branch['context_seconds']/period)
  args=dict(branch,tail=context_frames if branch['side']=='tail'else branch['edge_frames'],head=branch['edge_frames']if branch['side']=='tail'else context_frames)
  if _base39._base._core(d,args):branches.append(branch['name'])
 return branches

def clock_branch(d,pair,evidence,qualification,p=PARAMETERS):
 """Explicit sealed, source-bound experimental clock route; unqualified=UNKNOWN."""
 def unknown(reason):return {'status':'unknown','reason':reason,'nomination':False}
 if not p.get('clock_qualification_sha256'):return unknown('clock_qualification_not_pinned')
 if not isinstance(qualification,dict)or not isinstance(evidence,dict):return unknown('no_sealed_clock_qualification')
 digest=lambda x:hashlib.sha256(__import__('json').dumps(x,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 if digest(qualification)!=p['clock_qualification_sha256']:return unknown('clock_qualification_receipt_mismatch')
 if qualification.get('source_backed_clock_qualification')is not True or qualification.get('review_only')is not True:return unknown('clock_qualification_incomplete')
 if digest(evidence)not in qualification.get('sealed_evidence_sha256s',[]):return unknown('unsealed_clock_evidence')
 if not pair:return unknown('missing_source_bindings')
 bindings=[{k:pair[s][k]for k in ['clip_id','sha256','size_bytes']}for s in ['prev','next']]
 if evidence.get('source_bindings')!=bindings:return unknown('clock_source_mismatch')
 for k in ['source_sha256_verified','native_frame_pts_verified','advancing_ticks_each_side','independent_readers_agree','local_date_consensus','source_decode_verified']:
  if evidence.get(k)is not True:return unknown('missing_'+k)
 if d.get('db_gap_seconds')!=0 or d.get('reason')not in {'continuous','keyframe_boundary_continuous','source_adjacent','frame_jump','frame_boundary_ambiguous','frame_low_motion'}or d.get('gop_lattice',{}).get('status')!='complete' or d.get('source_evidence',{}).get('status')not in ['adjacent','unavailable']:return unknown('metadata_codec_or_replay_hold')
 interval=evidence.get('lost_time_interval_seconds')
 if not isinstance(interval,list)or len(interval)!=2 or any(type(x)not in(int,float)or not math.isfinite(x)for x in interval)or interval[0]>interval[1]:return unknown('invalid_clock_interval')
 low,high=interval
 if low < -p['clock_max_replay_seconds']or high>p['clock_max_positive_gap_seconds']:return {'status':'clock_gap_or_replay','nomination':False,'lost_time_interval_seconds':interval}
 return {'status':'clock_bounded_within_one_second_review','nomination':True,'lost_time_interval_seconds':interval}

def nominate(d,p=PARAMETERS,pair=None,clock_evidence=None,clock_qualification=None):
 return bool(visual_branches(d,p))or clock_branch(d,pair,clock_evidence,clock_qualification,p)['nomination']

def decision(d,pair=None,human_split_bindings=(),clock_evidence=None,clock_qualification=None):
 source_sha_pair=tuple(pair[s]['sha256']for s in ['prev','next'])if pair else None
 held=source_sha_pair in {tuple(x)for x in human_split_bindings}
 visual=visual_branches(d);clock=clock_branch(d,pair,clock_evidence,clock_qualification)
 return {'version':VERSION,'decision':'held_human_split'if held else 'join_candidate'if visual or clock['nomination']else 'not_nominated','visual_branches':visual,'clock':clock,'human_split_hold':held,'review_only':True,'automatic_join_authorized':False}
