import os,json,subprocess,base64,hashlib,urllib.request
env=os.environ.copy()
endpoint='https://'+env['R2_ACCOUNT_ID']+'.r2.cloudflarestorage.com'
bucket=env['R2_BUCKET']
# The existing R2 consumer uses curl's native SigV4 and environment expansion:
# credential contents never appear in argv, stdout, files, or CI.
def metadata_request(method,key,body=None,headers=()):
 url=endpoint+'/'+bucket+'/'+key
 args=['curl','--silent','--show-error','--connect-timeout','10','--max-time','45','--aws-sigv4','aws:amz:auto:s3','--variable','%R2_ACCESS_KEY_ID','--variable','%R2_SECRET_ACCESS_KEY','--expand-user','{{R2_ACCESS_KEY_ID}}:{{R2_SECRET_ACCESS_KEY}}','--write-out',chr(10)+'%{http_code}','--request',method]
 if body is not None:args+=['--header','Content-MD5: '+base64.b64encode(hashlib.md5(body).digest()).decode(),'--data-binary','@-']
 for header in headers:args+=['--header',header]
 args.append(url)
 p=subprocess.run(args,input=body,capture_output=True,env=env,timeout=50)
 if p.returncode:raise RuntimeError('R2 metadata request failed without changing deletion state')
 data,code=p.stdout.rsplit(bytes([10]),1)
 return data,int(code)
def immutable(key,body,content_type):
 _,code=metadata_request('PUT',key,body,('If-None-Match: *','Content-Type: '+content_type))
 assert code in (200,204,412),'R2 metadata conditional write failed: '+str(code)
 got,status=metadata_request('GET',key)
 assert status==200 and got==body,'R2 metadata full readback differs'
 return code!=412


import posixpath,datetime,collections,functools,concurrent.futures,importlib.util,re,time
POLICY_SHA='8a733e625a159942632617131b1388e19ce385e770a00363a5f79165042c076d'
INDEX_SHA='65682d111cb0419bbb28ddde8cc51159d2c5a4a4317a4db3c1ad1c8afc014240'
INDEX_KEY='staging/best14-20261021/nas-retention-20261025/frozen-index-20261102/'+INDEX_SHA+'/index.json.gz'
POLICY_KEY='staging/best14-20261021/nas-retention-20261025/cleanup-policy-20261102/'+POLICY_SHA+'/policy.json.gz'
PREFIX='staging/best14-20261021/nas-retention-20261025/full-cleanup-20261102/'
import gzip
def get_json(key,sha):
 raw,status=metadata_request('GET',key)
 assert status==200 and hashlib.sha256(raw).hexdigest()==sha,'immutable manifest input differs'
 return json.loads(gzip.decompress(raw))
index=get_json(INDEX_KEY,INDEX_SHA);policy=get_json(POLICY_KEY,POLICY_SHA)
assert index['source_audit_sha256']=='480d85a49288d79e201b14405dc72d0f6fc979a0fd06f1b7a95663053583865b'
assert len(index['parents'])==935 and index['entry_totals']['entries']==2943864

SUPPORTED_MANIFEST_COMPILERS=('full-cleanup-v1','full-cleanup-v2-strict-time')
def cleanup_recipe(compiler):
 if compiler not in SUPPORTED_MANIFEST_COMPILERS:
  raise ValueError('unsupported manifest compiler')
 return hashlib.sha256(json.dumps({'index':INDEX_SHA,'policy':POLICY_SHA,'compiler':compiler},sort_keys=True).encode()).hexdigest()
def checked_manifest_recipe(manifest):
 recipe=manifest.get('recipe')
 if recipe not in {cleanup_recipe(c) for c in SUPPORTED_MANIFEST_COMPILERS}:
  raise ValueError('manifest recipe is not bound to the reviewed index and policy')
 return recipe
whole=set(policy['whole_stream_keep']);stream_policy={x['stream_id']:x for x in policy['streams']}
def instant(x):
 if not isinstance(x,str):
  raise ValueError('timestamp must be an RFC3339 string')
 match=re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:\d{2})',x)
 if match is None:
  raise ValueError('timestamp must include an explicit timezone and at most six fractional digits')
 if match[3]!='Z' and (int(match[3][1:3])>23 or int(match[3][4:6])>59):
  raise ValueError('timestamp offset is outside RFC3339 bounds')
 fraction='.'+match[2].ljust(6,'0') if match[2] else ''
 offset='+00:00' if match[3]=='Z' else match[3]
 return datetime.datetime.fromisoformat(match[1]+fraction+offset).astimezone(datetime.timezone.utc)
windows=collections.defaultdict(list)
for w in policy['windows_utc']:windows[w['stream_id']].append((instant(w['start']),instant(w['end_exclusive'])))
def outside(sid,a,b):
 try:a,b=instant(a),instant(b)
 except Exception:return False
 return sid not in whole and a.year>=2020 and b.year<2028 and b>a and not any(a<end and b>start for start,end in windows[sid])
spec=importlib.util.spec_from_file_location('nas_ro','/Users/jelly/.stoarama/nas-storage-dashboard/app/refresh.py');pg=importlib.util.module_from_spec(spec);spec.loader.exec_module(pg)
def query(sql):
 p=subprocess.run(['/opt/homebrew/opt/libpq/bin/psql','-XqAt','-v','ON_ERROR_STOP=1'],input=("BEGIN READ ONLY;SET LOCAL statement_timeout='30s';"+sql+";ROLLBACK;").encode(),env=pg.pg_env(),capture_output=True,timeout=40)
 if p.returncode:raise RuntimeError('Read-only catalogue query failed: '+p.stderr.decode()[:400])
 return json.loads(p.stdout)
def paths_cte(paths):
 encoded=base64.b64encode(json.dumps(paths,separators=(',',':')).encode()).decode()
 return "paths AS MATERIALIZED(SELECT jsonb_array_elements_text(convert_from(decode('"+encoded+"','base64'),'UTF8')::jsonb) path)"
@functools.lru_cache(maxsize=2)
def references(sid):
 sid=str(int(sid))
 return query("""WITH refs AS MATERIALIZED(
 SELECT DISTINCT (p.item->'clip'->>'clip_id')::bigint clip_id,p.item->>'disposition' disposition
 FROM recording_collation_hours h JOIN recordings r ON r.id=h.recording_id CROSS JOIN LATERAL jsonb_array_elements(h.manifest->'clips')p(item)
 WHERE r.account_id=47 AND r.stream_id="""+sid+"""),
 joined AS MATERIALIZED(SELECT DISTINCT j.clip_id FROM recording_joined_source_snapshots j JOIN recording_clips c ON c.id=j.clip_id JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=47 AND r.stream_id="""+sid+""")
 SELECT json_build_object('collation',(SELECT coalesce(json_agg(DISTINCT clip_id),'[]') FROM refs),'quarantined',(SELECT coalesce(json_agg(DISTINCT clip_id),'[]') FROM refs WHERE disposition='quarantined'),'joined',(SELECT coalesce(json_agg(clip_id),'[]') FROM joined))""")
def catalog(paths,joined):
 if not paths:return []
 cte=paths_cte([p[7:] if joined else p for p in paths])
 if joined:
  return query("WITH "+cte+""" SELECT coalesce(json_agg(x),'[]') FROM(
 SELECT o.id source_id,'collated' kind,h.recording_id,r.stream_id,'joined/'||o.nas_relative_path path,o.size_bytes,lower(o.sha256) sha256,'stoarama' bucket,o.object_key,o.start_at,o.end_at,o.source_clip_ids source_ids,coalesce(nullif(s.local_timezone,''),r.cron_timezone) timezone,true catalog_recovery_active,(ack.sha256=o.sha256 AND ack.size_bytes=o.size_bytes) inventory_matches
 FROM paths p JOIN recording_collation_outputs o ON o.nas_relative_path=p.path
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id JOIN streams s ON s.id=r.stream_id
 LEFT JOIN recording_collation_output_acks ack ON ack.output_id=o.id AND ack.connection_id=13
 WHERE r.account_id=47)x""")
 return query("WITH "+cte+""" SELECT coalesce(json_agg(x),'[]') FROM(
 SELECT c.id source_id,'raw' kind,c.recording_id,r.stream_id,c.display_path path,c.size_bytes,lower(c.sha256) sha256,c.bucket,c.object_key,c.clip_start_at start_at,c.clip_end_at end_at,ARRAY[c.id] source_ids,coalesce(nullif(s.local_timezone,''),r.cron_timezone) timezone,(c.purged_at IS NULL) catalog_recovery_active,(f.state='present' AND f.relative_path=c.display_path AND f.sha256=lower(c.sha256) AND f.size_bytes=c.size_bytes) inventory_matches
 FROM paths p JOIN nas_inventory_files f ON f.connection_id=13 AND f.relative_path=p.path
 JOIN recording_clips c ON c.id=f.clip_id JOIN recordings r ON r.id=c.recording_id JOIN streams s ON s.id=r.stream_id
 WHERE r.account_id=47)x""")
def sources(ids):
 if not ids:return {}
 array=','.join(str(int(n)) for n in sorted(set(ids)))
 rows=query("""SELECT coalesce(json_agg(x),'[]') FROM(
 SELECT c.id,r.stream_id,c.clip_start_at start_at,c.clip_end_at end_at,coalesce(nullif(s.local_timezone,''),r.cron_timezone) timezone,c.bucket,c.size_bytes,lower(c.sha256) sha256,c.object_key
 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47 AND c.id=ANY(ARRAY["""+array+"""]::bigint[]))x""")
 return {r['id']:r for r in rows}
def head_batch(rows):
 if not rows:return {}
 out={}
 for begin in range(0,len(rows),128):
  part=rows[begin:begin+128]
  args=['curl','--silent','--show-error','--connect-timeout','10','--max-time','30','--aws-sigv4','aws:amz:auto:s3','--variable','%R2_ACCESS_KEY_ID','--variable','%R2_SECRET_ACCESS_KEY','--expand-user','{{R2_ACCESS_KEY_ID}}:{{R2_SECRET_ACCESS_KEY}}','--head','--parallel','--parallel-max','16','--write-out','{"request":%{json},"headers":%{header_json}}'+chr(10)]
  for row in part:
   # Only the configured private bucket/endpoint. Never follow a redirect.
   import urllib.parse
   args+=['--output','/dev/null',endpoint+'/'+bucket+'/'+urllib.parse.quote(row['object_key'],safe='/~')]
  p=subprocess.run(args,env=env,capture_output=True,timeout=45)
  text=p.stdout.decode();decoder=json.JSONDecoder();offset=0;seen=set()
  while offset<len(text):
   while offset<len(text) and text[offset].isspace():offset+=1
   if offset==len(text):break
   obj,offset=decoder.raw_decode(text,offset);n=obj['request']['urlnum']
   assert isinstance(n,int) and 0<=n<len(part) and n not in seen
   seen.add(n);h=obj['headers'];code=obj['request']['http_code']
   length=h.get('content-length',[None])[0]
   out[(part[n]['kind'],part[n]['source_id'])]={'http':code,'size_bytes':int(length) if length and str(length).isdigit() else None,'etag':h.get('etag',[''])[0],'observed_date':h.get('date',[''])[0]}
  for n,row in enumerate(part):
   if n not in seen:out[(row['kind'],row['source_id'])]={'http':0,'size_bytes':None,'etag':''}
 return out
def frozen_valid(e):
 f=e.get('frozen',[])
 return e.get('kind')=='file' and not e.get('error') and not e.get('path_bytes_b64') and e.get('links')==1 and len(f)==5 and f[0]>0 and f[3]>0 and f[4]>0
def valid_identity(row,sid,entry):
 return row['stream_id']==sid and row['timezone']==stream_policy[sid]['timezone'] and row['path']==entry['path'] and row['size_bytes']==entry['frozen'][0] and row['size_bytes']>0 and row['bucket']=='stoarama' and bool(row['object_key']) and bool(re.fullmatch('[a-f0-9]{64}',row['sha256'] or '')) and row['inventory_matches'] is True
import urllib.request,urllib.error
FULL_PREFIX='staging/best14-20261021/nas-retention-20261025/full-cleanup-20261102/'
ordinals={name:n for n,name in enumerate(sorted(x['path'] for x in index['folder_totals']),1)}
folder_map={f['path']:f for f in index['folder_totals']}
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):raise RuntimeError('Operator redirect refused')
operator=urllib.request.build_opener(NoRedirect())
def service(method,path,body=None):
 req=urllib.request.Request('https://stoarama.com/api/v1/service/nas-cleanup/'+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Authorization':'Bearer '+os.environ['SERVICE_TOKEN'],'User-Agent':'Stoarama-Operator/1.0','Content-Type':'application/json'})
 with operator.open(req,timeout=180) as response:
  data=response.read(12*1024*1024+1);assert len(data)<=12*1024*1024
  return json.loads(data)
assert service('GET','plans/6')['master_deletion_enabled'] is False
receipts=[];retained=[];created=0
def confirm(detail,rows):
 assert detail['mode']=='dry_run' and detail['master_deletion_enabled'] is False and detail['allow_nas_only'] is False
 actual={(i['kind'],i['source_id']):i for i in detail['items']}
 assert set(actual)=={(r['kind'],r['source_id']) for r in rows}
 for r in rows:
  i=actual[(r['kind'],r['source_id'])]
  assert i['relative_path']==r['path'] and i['sha256']==r['sha256'] and i['size_bytes']==r['logical_bytes'] and i['stream_id']==r['stream_id']
  assert instant(i['start_at'])==instant(r['start_at']) and instant(i['end_at'])==instant(r['end_at']) and i['timezone']==r['timezone']
def lookup(label):
 rows=query("SELECT coalesce(json_agg(x),'[]') FROM(SELECT id,label,plan_sha256,mode FROM nas_cleanup_plans WHERE account_id=47 AND connection_id=13 AND label='"+label+"')x")
 return rows[0] if rows else None
def prepare(label,rows,parent):
 global created
 result=existing.get(label)
 if result is None:
  started=time.monotonic()
  try:result=service('POST','plans',{'label':label,'items':[{'kind':r['kind'],'source_id':r['source_id']} for r in rows]})
  except Exception as exc:
   result=lookup(label)
   if result is None:
    code=getattr(exc,'code',None)
    if code==409:
     if len(rows)>1:
      middle=len(rows)//2;prepare(label+'-s0',rows[:middle],parent);prepare(label+'-s1',rows[middle:],parent);return
     retained.append({'parent':parent,'kind':rows[0]['kind'],'source_id':rows[0]['source_id'],'reason':'admission409; retained for review'});return
    raise RuntimeError('Cleanup admission not confirmed; inspect existing label before retry: '+label)
  created+=1
  admission_ms=round((time.monotonic()-started)*1000)
 else:admission_ms=0
 detail=service('GET','plans/'+str(result['id']))
 try:confirm(detail,rows)
 except Exception:
  service('POST','plans/'+str(result['id'])+'/control',{'mode':'paused','plan_sha256':detail['plan_sha256'],'reason':'prepared catalogue differs from full manifest; retain pending review','allow_nas_only':False})
  raise RuntimeError('Prepared manifest changed; exact plan paused without deletion')
 receipts.append({'id':detail['id'],'label':label,'plan_sha256':detail['plan_sha256'],'files':len(rows),'logical_bytes':sum(r['logical_bytes'] for r in rows),'parent':parent,'admission_ms':admission_ms,'mode':'dry_run'})
 if len(receipts)%10==0:print(json.dumps({'phase':'preparing_full_dryruns','plans':len(receipts),'files':sum(x['files'] for x in receipts),'logical_bytes':sum(x['logical_bytes'] for x in receipts),'last_admission_ms':admission_ms,'deletion_enabled':False}),flush=True)

import sys
assert len(sys.argv)==2,'Usage: prepare_full_manifest.py <review manifest key>'
manifest_key=sys.argv[1]
assert re.fullmatch(re.escape(FULL_PREFIX)+r'(?:timestamp-repair-1/)?[a-f0-9]{64}/manifest.json.gz',manifest_key)
manifest_sha=manifest_key.split('/')[-2]
manifest=get_json(manifest_key,manifest_sha)
assert manifest['schema']=='nas-cleanup-full-manifest-v1'
assert manifest['policy_sha256']==POLICY_SHA and manifest['source_frozen_index_sha256']==INDEX_SHA
assert manifest['execution_enabled'] is False and manifest['allow_nas_only'] is False and manifest['actual_deleted_files']==0
recipe=checked_manifest_recipe(manifest)
existing=query("SELECT coalesce(json_agg(x),'[]') FROM(SELECT id,label,plan_sha256,mode FROM nas_cleanup_plans WHERE account_id=47 AND connection_id=13 AND label LIKE 'nas-full-"+recipe[:12]+"-%')x")
existing={p['label']:p for p in existing}
assert sum(f['files'] for f in manifest['folders'])==index['entry_totals']['file_entries']
assert sum(f['logical_bytes'] for f in manifest['folders'])==index['entry_totals']['regular_file_bytes']
folder_refs={f['parent']:f for f in manifest['folders']}
assert len(folder_refs)==len(index['folder_totals'])
groups=manifest['dry_run_groups'];seen_labels=set();seen_ids=set()
cache_parent=None;cache_rows=None
for group in groups:
 label=group['label'];parent=group['parent']
 assert re.fullmatch(r'nas-full-'+re.escape(recipe[:12])+r'-(?:ts-)?[0-9]+-[0-9]+',label)
 assert label not in seen_labels and parent in index['parents'] and index['parents'][parent]['owner_stream_id'] not in whole
 seen_labels.add(label)
 identities=[(i['kind'],i['source_id']) for i in group['items']]
 assert 1<=len(identities)<=500 and len(set(identities))==len(identities) and not seen_ids.intersection(identities)
 seen_ids.update(identities)
 if cache_parent!=parent:
  ref=folder_refs[parent];obj=get_json(ref['key'],ref['sha256'])
  assert obj['parent']==parent and obj['policy_sha256']==POLICY_SHA and obj['frozen_index_sha256']==INDEX_SHA
  assert obj['stream_id']==index['parents'][parent]['owner_stream_id']
  assert obj['counts']==ref['counts'] and len(obj['records'])==ref['files'] and sum(r['logical_bytes'] for r in obj['records'])==ref['logical_bytes']
  cache_rows={(r['kind'],r['source_id']):r for r in obj['records'] if r['status']=='PENDING_NAS_AND_R2_FULL_SHA_VERIFICATION'}
  cache_parent=parent
 rows=[cache_rows[i] for i in identities]
 assert sum(r['logical_bytes'] for r in rows)==group['logical_bytes']
 prepare(label,rows,parent)
summary={'schema':'nas-full-preparation-receipts-v1','recipe':recipe,'source_manifest_key':manifest_key,'source_manifest_sha256':manifest_sha,'policy_sha256':POLICY_SHA,'frozen_index_sha256':INDEX_SHA,'completed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'plans':receipts,'retained_during_admission':retained,'deletion_enabled':False}
blob=gzip.compress(json.dumps(summary,sort_keys=True,separators=(',',':')).encode(),mtime=0);sha=hashlib.sha256(blob).hexdigest();key=FULL_PREFIX+sha+'/prepare-receipts.json.gz'
immutable(key,blob,'application/gzip')
print(json.dumps({'phase':'ALL_FULL_GROUPS_QUEUED','receipt_key':key,'receipt_sha256':sha,'source_manifest_sha256':manifest_sha,'plans':len(receipts),'new_plans':created,'files':sum(x['files'] for x in receipts),'logical_bytes':sum(x['logical_bytes'] for x in receipts),'retained_during_admission':len(retained),'deletion_enabled':False}),flush=True)
