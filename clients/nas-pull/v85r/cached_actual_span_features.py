"""Read-only authenticated actual-span feature adapter; no decoding or join authority."""
import json,hashlib,datetime,math
VERSION='v89l-authenticated-cached-actual-span-features-research-20261010'
CACHE_SHA='3629dc605249e622d68b551279db168ba156c307451cc90133a901f102d767a5'
TOOLS={'/opt/collate/lookahead-control-20261009/queue-evaluator-linux':'edd5ad0cbba75444489d2d1cbd62ed6c6bc9b38adaca38c1819f1d8ae57aeb00','/opt/stitch-proof/bin/ffmpeg':'2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b','/opt/stitch-proof/bin/ffprobe':'9b2bac862606a48f5bc36091d38e5334cf241538a6f4cc47c60f68e7973a365c','/opt/collate/decision-endpoint-pilot-20261009/ffmpeg-bitexact':'0bfe856e67c730f8789c84e14927e9844ff787d33eaf6036624b363030877acf'}
CONTRACT='temporary_video_only_pending_audiofill'
def key(bindings):return tuple((b['clip_id'],b['sha256'],b['size_bytes'])for b in bindings)
def stamp(s):
 # Padding fractional seconds avoids implementations interpreting .8 as .000008.
 s=s.replace('Z','+00:00')
 if '.'in s:
  a,b=s.split('.',1);f,z=b.split('+',1);s=a+'.'+f.ljust(6,'0')+'+'+z
 return datetime.datetime.fromisoformat(s).timestamp()
def _member_hash(value):return hashlib.sha256(json.dumps(value,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def select(row,bad_source_shas,human_veto_keys):
 """Returns a replacement diagnostic only; source-clock/audio stay UNKNOWN."""
 b=row['source_bindings'];assert len(b)==2
 if any(x['sha256']in bad_source_shas for x in b):return {'status':'HOLD_observed_whole_source_failure'}
 if tuple((x['sha256'],x['size_bytes'])for x in b)in {tuple((v[1],v[2])for v in k)for k in human_veto_keys}:return {'status':'HOLD_human_source_veto'}
 q=row.get('actual_span_qualification');n=row.get('native_actual_span');orig=row.get('native_original')
 if not(q and n and orig):return {'status':'UNKNOWN_no_authenticated_actual_span_context'}
 assert row['full_R2_sha256_readback_verified']is True and row['production_authorized']is False
 assert q['qualified']is True and q['requested_nominal_seconds_retained']==[60,60]
 assert q['video_only_contract']==CONTRACT and q['source_clock_qualification']=='UNKNOWN'
 assert q['automatic_join_authorized']is False and q['production_authorized']is False
 assert n['source_bindings']==orig['source_bindings']==b and n['tool_bindings']==orig['tool_bindings']==TOOLS
 members={x['name']:x['sha256']for x in row['members']}
 for name,value in [('capture-span-qualification.private.json',q),('counterfactual-registered-actual-span-native.private.json',n),('original-requested60-native.private.json',orig)]:assert members[name]==_member_hash(value),'Native/qualification member differs from authenticated archive'
 assert all(name in members for name in ['fullsource-actualPTS.private.json','prev-decoded-source-evidence.private.json','next-decoded-source-evidence.private.json','shared-scratch-release.private.json'])
 spans=[stamp(x['end_utc'])-stamp(x['start_utc'])for x in b]
 assert all(math.isfinite(v)and v>=20 for v in spans)
 assert all(abs(a-c)<=.001 for a,c in zip(spans,q['expected_source_spans_seconds']))
 assert abs(stamp(b[1]['start_utc'])-stamp(b[0]['end_utc']))<=.001
 d=n['native']['diagnostic_window20'];od=orig['native']['diagnostic_window20']
 assert [d['prev_clip_id'],d['next_clip_id']]==[x['clip_id']for x in b]
 assert [od['prev_clip_id'],od['next_clip_id']]==[x['clip_id']for x in b]
 assert od['reason']in('prev_clip_cut_short','next_clip_cut_short','content_span_mismatch')
 return {'status':'AUTHENTICATED_actual_span_feature_replacement','source_bindings':b,'diagnostic_window20':d,'original_diagnostic_window20':od,'original_requested_seconds':[60,60],'actual_expected_seconds':spans,'archive_sha256':row['archive_sha256'],'source_clock':'UNKNOWN','audio_continuity':'UNKNOWN','automatic_join_authorized':False,'production_authorized':False}
def load(raw,bad_source_shas,human_veto_keys):
 assert hashlib.sha256(raw).hexdigest()==CACHE_SHA,'Cache differs from finite full-R2-readback snapshot'
 assert raw.endswith(b'\n');seen=set();out=[]
 for line in raw.splitlines():
  r=json.loads(line);k=key(r['source_bindings']);assert k not in seen;seen.add(k);out.append(select(r,bad_source_shas,human_veto_keys))
 return out
