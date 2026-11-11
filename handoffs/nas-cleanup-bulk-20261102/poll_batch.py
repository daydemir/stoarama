import os,json,pathlib,urllib.request,datetime,collections
base=pathlib.Path('/Users/jelly/Build/stoarama/handoffs/nas-cleanup-bulk-20261102')
admission=json.loads((base/'batch-admission.json').read_text())
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):raise RuntimeError('Operator redirect refused')
opener=urllib.request.build_opener(NoRedirect())
request=urllib.request.Request('https://stoarama.com/api/v1/service/nas-cleanup/plans/'+str(admission['id']),headers={'Authorization':'Bearer '+os.environ['SERVICE_TOKEN'],'User-Agent':'Stoarama-Operator/1.0'})
with opener.open(request,timeout=45) as response:
 data=response.read(10485761);assert len(data)<=10485760;x=json.loads(data)
assert x['plan_sha256']==admission['plan_sha256'] and x['id']==5
assert x['mode']=='dry_run' and not x['master_deletion_enabled'] and not x['allow_nas_only']
assert len(x['items'])==128 and len(x['results'])==128
assert all(not r['delete_ever_issued'] for r in x['results'])
(base/'batch-report-current.json').write_text(json.dumps(x,indent=2)+'\n')
by=collections.defaultdict(lambda:{'files':0,'logical_bytes':0})
reasons=collections.Counter();folders={}
for r in x['results']:
 i=x['items'][r['ordinal']-1]
 disposition='eligible_r2_verified' if r['state']=='would_delete' and r['recovery']=='r2_sha_verified' else r['state']
 if r['state']=='would_delete' and r['recovery']!='r2_sha_verified':disposition='retained_recovery_'+r['recovery']
 by[disposition]['files']+=1;by[disposition]['logical_bytes']+=i['size_bytes']
 if r['policy_reason']:reasons[r['policy_reason']]+=1
 if disposition=='eligible_r2_verified':
  parent=i['relative_path'].rsplit('/',1)[0]
  f=folders.setdefault(parent,{'folder':parent,'stream_id':i['stream_id'],'raw_files':0,'raw_bytes':0,'stitched_files':0,'stitched_bytes':0,'logical_bytes':0,'files':[]})
  k='raw' if i['kind']=='raw' else 'stitched';f[k+'_files']+=1;f[k+'_bytes']+=i['size_bytes'];f['logical_bytes']+=i['size_bytes']
  f['files'].append({'kind':i['kind'],'source_id':i['source_id'],'name':i['relative_path'].rsplit('/',1)[1],'bytes':i['size_bytes'],'sha256':i['sha256']})
now=datetime.datetime.now(datetime.timezone.utc).isoformat()
terminal=sum(r['state'] not in ('pending','leased') for r in x['results'])
summary={'plan_id':x['id'],'plan_sha256':x['plan_sha256'],'checked_at':now,'completed':terminal,'total':128,'percent':round(terminal/128*100,1),'states':x['states'],'dispositions':dict(by),'hold_reasons':dict(reasons),'eligible_folder_count':len(folders),'master_deletion_enabled':False,'allow_nas_only':False,'actual_deleted_files':0,'full_NAS_dryrun':False}
(base/'batch-summary-current.json').write_text(json.dumps(summary,indent=2)+'\n')
(base/'eligible-folders-current.json').write_text(json.dumps({'summary':summary,'folders':sorted(folders.values(),key=lambda f:(f['stream_id'],f['folder']))},indent=2)+'\n')
state=json.loads((base/'state.json').read_text());state.update(status='NAS_DRY_RUN_COMPLETE' if terminal==128 else 'NAS_DRY_RUN_RUNNING',updated_at=now,completed=terminal,dispositions=dict(by))
(base/'state.json').write_text(json.dumps(state,indent=2)+'\n')
if terminal==128:
 for n in ('batch-report','batch-summary','eligible-folders'):
  (base/(n+'-final.json')).write_bytes((base/(n+'-current.json')).read_bytes())

lines=['NAS cleanup dry-run report', 'Checked: '+now, 'Plan: '+str(x['id'])+' / '+x['plan_sha256'], '',
'DELETION OFF. No files deleted. NAS-only and uncertain content retained.',
'This report covers128 explicitly selected files, not the whole NAS.',
'Folder rows summarize ONLY the listed files; they do not authorize removing folders or other files.',
'Sizes are logical bytes / decimal GB, not guaranteed reclaimed filesystem capacity.', '',
'PROGRESS: '+str(terminal)+'/128 files checked', '', 'RESULTS']
for disposition,v in sorted(by.items()):
 lines.append('%s: %d files / %.6f GB / %d bytes' % (disposition,v['files'],v['logical_bytes']/1e9,v['logical_bytes']))
lines+=['', 'ELIGIBLE FILES BY FOLDER (full NAS and R2 SHA verified)']
for f in sorted(folders.values(),key=lambda z:(z['stream_id'],z['folder'])):
 lines.append('Stream %s | %.6f GB | raw %d files / %.6f GB | stitched %d files / %.6f GB' % (f['stream_id'],f['logical_bytes']/1e9,f['raw_files'],f['raw_bytes']/1e9,f['stitched_files'],f['stitched_bytes']/1e9))
 lines.append(f['folder'])
 for item in f['files']:lines.append('  %s:%s | %d bytes | %s' % (item['kind'],item['source_id'],item['bytes'],item['name']))
 lines.append('')
lines+=['RETAINED OR UNFINISHED FILES']
for r in x['results']:
 if r['state']=='would_delete' and r['recovery']=='r2_sha_verified':continue
 i=x['items'][r['ordinal']-1]
 lines.append('%s:%s | %s | %s | %d bytes | %s' % (i['kind'],i['source_id'],r['state'],r['recovery'],i['size_bytes'],r['policy_reason'] or (r.get('outcome') or {}).get('error','')))
 lines.append('  '+i['relative_path'])
(base/'review-current.txt').write_text('\n'.join(lines)+'\n')
if terminal==128:(base/'review-final.txt').write_bytes((base/'review-current.txt').read_bytes())

print(json.dumps({'checked_at':now,'completed':terminal,'total':128,'percent':summary['percent'],'dispositions':dict(by),'eligible_folders':len(folders),'hold_reasons':dict(reasons),'deletion_off':True}))

