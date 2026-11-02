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

pool=concurrent.futures.ThreadPoolExecutor(max_workers=4)
root_folders={f['path']:f for f in index['folder_totals']}
global_counts=collections.defaultdict(lambda:{'files':0,'logical_bytes':0})
results=[];candidate_groups=[];started_at=datetime.datetime.now(datetime.timezone.utc).isoformat()
recipe=cleanup_recipe('full-cleanup-v2-strict-time')
def add_count(counts,status,n,size):
 counts[status]['files']+=n;counts[status]['logical_bytes']+=size
def read_parent(parent,info):
 def one(ref):
  raw,status=metadata_request('GET',ref['key'])
  assert status==200 and len(raw)==ref['bytes'] and hashlib.sha256(raw).hexdigest()==ref['sha256']
  obj=json.loads(gzip.decompress(raw));assert obj['parent']==parent and len(obj['entries'])==ref['entries']
  return obj['entries']
 entries=[]
 for part in pool.map(one,info['shards']):entries.extend(part)
 assert len(entries)==info['entries']
 seen=set()
 for e in entries:
  assert posixpath.dirname(e['path'])==parent and e['path'] not in seen,'ambiguous indexed namespace'
  seen.add(e['path'])
 return entries
def process_parent(parent,info,folder):
 counts=collections.defaultdict(lambda:{'files':0,'logical_bytes':0})
 entries=read_parent(parent,info)
 files=[e for e in entries if e['kind']=='file']
 assert len(files)==folder['file_count'] and sum(e['frozen'][0] for e in files)==folder['logical_bytes']
 entry_by_path={e['path']:e for e in entries}
 paths=[e['path'] for e in files if e['path'].lower().endswith('.mp4')]
 records={e['path']:{'path':e['path'],'logical_bytes':e['frozen'][0],'status':'RETAIN_METADATA_OR_UNSUPPORTED'} for e in files}
 sid=info['owner_stream_id'];head_candidates=[]
 try:
  rows=catalog(paths,parent.startswith('joined/'));by_path=collections.defaultdict(list)
  for row in rows:by_path[row['path']].append(row)
  refs=references(sid);joined_refs=set(refs['joined']);all_refs=set(refs['collation'])|joined_refs;error_refs=set(refs['quarantined'])|joined_refs
  source_ids=[n for r in rows if r['kind']=='collated' for n in r['source_ids']]
  src=sources(source_ids)
 except Exception as exc:
  for path in paths:records[path]['status']='RETAIN_CATALOGUE_QUERY_UNCERTAIN'
  return finish_parent(parent,sid,records,counts,[],str(exc)[:400])
 for path in paths:
  record=records[path];entry=entry_by_path[path];options=by_path[path]
  if not frozen_valid(entry):record['status']='RETAIN_FROZEN_IDENTITY_UNCERTAIN';continue
  if len(path.encode('utf-8'))>1024 or path.startswith(('/','#','@')) or any(p in ('','.','..') for p in path.split('/')) or any(c in path for c in ('\\','\0','\r','\n')):
   record['status']='RETAIN_UNSAFE_OR_UNSUPPORTED_PATH';continue
  if len(options)!=1:record['status']='RETAIN_UNMATCHED_OR_AMBIGUOUS_CATALOGUE';continue
  row=options[0]
  record.update(kind=row['kind'],source_id=row['source_id'],sha256=row['sha256'],stream_id=row['stream_id'],start_at=row['start_at'],end_at=row['end_at'],timezone=row['timezone'])
  if not valid_identity(row,sid,entry):record['status']='RETAIN_CATALOGUE_IDENTITY_UNCERTAIN';continue
  if not outside(sid,row['start_at'],row['end_at']):record['status']='RETAIN_28_DAY_OR_UNKNOWN_BOUNDS';continue
  ids=row['source_ids']
  if not ids or len(ids)>10000 or len(set(ids))!=len(ids):record['status']='RETAIN_SOURCE_MEMBERSHIP_UNCERTAIN';continue
  source_rows=[row] if row['kind']=='raw' else [src.get(n) for n in ids]
  if any(s is None or s['stream_id']!=sid or s['timezone']!=stream_policy[sid]['timezone'] or s['bucket']!='stoarama' or not outside(sid,s['start_at'],s['end_at']) for s in source_rows):
   record['status']='RETAIN_SOURCE_28_DAY_OR_UNKNOWN_BOUNDS';continue
  if any(n in (all_refs if row['kind']=='raw' else error_refs) for n in ids):
   record['status']='RETAIN_CURRENT_SOURCE_OR_ERROR_REFERENCE';continue
  if row['kind']=='raw':
   side=entry_by_path.get(path+'.stoarama.json')
   if side is None or not frozen_valid(side) or side['frozen'][0]>1048576:
    record['status']='RETAIN_SIDECAR_UNCERTAIN';continue
  head_candidates.append(row)
 heads=head_batch(head_candidates)
 pending=[]
 for row in head_candidates:
  record=records[row['path']];proof=heads[(row['kind'],row['source_id'])]
  record['r2_head']=proof
  if proof['http']==404:record['status']='RETAIN_NAS_ONLY_AT_HEAD_CHECK'
  elif proof['http']!=200:record['status']='RETAIN_R2_RECOVERY_UNCERTAIN'
  elif proof['size_bytes']!=row['size_bytes']:record['status']='RETAIN_R2_SIZE_CONFLICT'
  else:
   record['status']='PENDING_NAS_AND_R2_FULL_SHA_VERIFICATION';pending.append(row)
   if row['kind']=='raw':records[row['path']+'.stoarama.json']['status']='BOUND_SIDECAR_PENDING_MAIN_FILE'
 return finish_parent(parent,sid,records,counts,pending,'')
def finish_parent(parent,sid,records,counts,pending,error):
 for record in records.values():add_count(counts,record['status'],1,record['logical_bytes'])
 obj={'schema':'nas-cleanup-full-parent-v1','parent':parent,'stream_id':sid,'policy_sha256':POLICY_SHA,'frozen_index_sha256':INDEX_SHA,'catalog_checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'counts':dict(counts),'records':sorted(records.values(),key=lambda r:r['path']),'query_error':error,'deletion_enabled':False}
 blob=gzip.compress(json.dumps(obj,sort_keys=True,separators=(',',':')).encode(),mtime=0);sha=hashlib.sha256(blob).hexdigest();key=PREFIX+'objects/'+sha+'/parent.json.gz'
 immutable(key,blob,'application/gzip')
 ref={'parent':parent,'stream_id':sid,'key':key,'sha256':sha,'bytes':len(blob),'counts':dict(counts),'logical_bytes':sum(v['logical_bytes'] for v in counts.values()),'files':sum(v['files'] for v in counts.values())}
 return ref,pending
for number,(parent,folder) in enumerate(sorted(root_folders.items()),1):
 info=index['parents'].get(parent)
 if info is None or info['owner_stream_id'] in whole:
  status='RETAIN_WHOLE_STREAM_UNCERTAINTY' if info is not None else 'RETAIN_OUTSIDE_REVIEWED_DELETE_NODES'
  counts={status:{'files':folder['file_count'],'logical_bytes':folder['logical_bytes']}}
  ref={'parent':parent,'stream_ids':folder['owner_stream_ids'],'counts':counts,'files':folder['file_count'],'logical_bytes':folder['logical_bytes'],'source_frozen_index_sha256':INDEX_SHA}
 else:
  ref,pending=process_parent(parent,info,folder)
  # Each batch remains exact files within one reviewed parent; never recursive.
  pending.sort(key=lambda r:(-r['size_bytes'],r['kind'],r['source_id']))
  for offset in range(0,len(pending),500):
   group=pending[offset:offset+500]
   candidate_groups.append({'label':'nas-full-'+recipe[:12]+'-'+str(number)+'-'+str(offset//500),'parent':parent,'logical_bytes':sum(r['size_bytes'] for r in group),'items':[{'kind':r['kind'],'source_id':r['source_id']} for r in group]})
 for status,value in ref['counts'].items():add_count(global_counts,status,value['files'],value['logical_bytes'])
 results.append(ref)
 if number%25==0:
  print(json.dumps({'phase':'catalogue_manifest','folders_done':number,'folders_total':len(root_folders),'pending_verification':global_counts.get('PENDING_NAS_AND_R2_FULL_SHA_VERIFICATION',{}),'nas_only':global_counts.get('RETAIN_NAS_ONLY_AT_HEAD_CHECK',{}),'deletion_enabled':False}),flush=True)
pool.shutdown(wait=True)
assert sum(v['files'] for v in global_counts.values())==index['entry_totals']['file_entries']
assert sum(v['logical_bytes'] for v in global_counts.values())==index['entry_totals']['regular_file_bytes']
manifest={'schema':'nas-cleanup-full-manifest-v1','recipe':recipe,'source_frozen_index_key':INDEX_KEY,'source_frozen_index_sha256':INDEX_SHA,'policy_key':POLICY_KEY,'policy_sha256':POLICY_SHA,'period_snapshot_at':policy['refreshed_at'],'started_at':started_at,'completed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'counts':dict(global_counts),'folders':results,'dry_run_groups':candidate_groups,'execution_enabled':False,'allow_nas_only':False,'actual_deleted_files':0,'limits':['Frozen inventory fromOctober31; new or changed files are retained until individually verified.','R2 HEAD establishes presence/size only; every candidate still requires full NAS and R2 SHA proof.','Unsupported, ambiguous, whole-stream, source-reference and NAS-only content remains retained.','Logical bytes are not guaranteed reclaimed filesystem capacity.']}
raw=gzip.compress(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode(),mtime=0);sha=hashlib.sha256(raw).hexdigest();key=PREFIX+sha+'/manifest.json.gz'
immutable(key,raw,'application/gzip')
print(json.dumps({'phase':'FULL_MANIFEST_COMPLETE','manifest_key':key,'manifest_sha256':sha,'manifest_bytes':len(raw),'recipe':recipe,'counts':dict(global_counts),'folders':len(results),'dry_run_groups':len(candidate_groups),'all_regular_files_accounted':index['entry_totals']['file_entries'],'all_regular_logical_bytes_accounted':index['entry_totals']['regular_file_bytes'],'deletion_enabled':False}),flush=True)
