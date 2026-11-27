#!/usr/bin/env python3
"""Download verified stitching outputs into the site's named folder tree.

Requires Python 3.9 or newer; no additional packages.
Save this script, then run the command in Terminal.
python3 reviewed_downloader.py --destination ./stitched
Rerun the same command to resume. Existing files are never overwritten.
If the server retires the dataset generation mid-run (HTTP 410), the download
continues on the current generation when it lists the same files (names, sizes,
checksums); otherwise it stops and says what changed.
"""
import argparse,collections,hashlib,json,os,re,sys,time,urllib.request,urllib.error,urllib.parse,unicodedata
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path,PurePosixPath
DEFAULT='https://stoarama.com'
USER_AGENT='Stoarama-Delivery/1.0 (+https://stoarama.com/global-street-scores-delivery)'

def relative_path(raw):
 if not isinstance(raw,str) or '\\' in raw or any(ord(c)<32 for c in raw):raise ValueError('Unsafe manifest path')
 p=PurePosixPath(raw)
 if any(len(v.encode('utf-8'))>250 for v in p.parts):raise ValueError('Path component too long')
 if p.is_absolute() or len(p.parts)!=4 or any(v in ('','.', '..') or v.endswith((' ','.')) for v in raw.split('/')):raise ValueError('Unsafe manifest path')
 if any(re.match(r'(?i)^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)',v) for v in p.parts):raise ValueError('Reserved manifest path')
 return p

def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()

def destination(root,raw):
 p=relative_path(raw);parent=root
 for part in p.parts[:-1]:
  parent=parent/part
  if parent.is_symlink():raise ValueError('Symlink in destination tree')
  parent.mkdir(exist_ok=True)
  if not parent.is_dir() or not parent.resolve().is_relative_to(root):raise ValueError('Destination escapes folder')
 return parent/p.name

def save_receipt(receipt,manifest):
 """Preserve earlier catalog receipts when verified delivery changes."""
 raw=json.dumps(manifest,ensure_ascii=False,indent=2,sort_keys=True).encode('utf-8')
 def matches(path):
  if path.is_symlink() or (path.exists() and not path.is_file()):raise ValueError('Non-regular manifest receipt refused')
  if not path.exists():return False
  try:return json.loads(path.read_text(encoding='utf-8'))==manifest
  except (ValueError,UnicodeError):return False
 if matches(receipt):return receipt
 target=receipt
 if receipt.exists():target=receipt.with_name(receipt.stem+'-'+hashlib.sha256(raw).hexdigest()+receipt.suffix)
 if matches(target):return target
 if target.exists():raise ValueError('Existing manifest receipt differs; left untouched')
 try:
  fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
 except FileExistsError:
  if matches(target):return target
  raise ValueError('Manifest receipt changed; left untouched')
 with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
 return target

FRESH='Copy a fresh command from the page to continue.'
RETIRED=object()
DATASET_API=None
API_TOKEN=None
DELIVERY_GENERATION=None

class GenerationRetired(Exception):
 """The access server answered 410: the generation this download uses is retired."""

class GenerationChanged(Exception):
 """The current generation delivers different files than the retired one."""

def access_request(base,path,generation=None):
 if DATASET_API:
  match=re.fullmatch(r'/api/(coverage-downloads|confident-repairs|reviewed-repairs)/ticket/([^/?]+)\?sha256=([0-9a-f]{64})(?:&path=([^&]+))?(?:&generation=([a-f0-9]{32}))?',path)
  if path.startswith(DATASET_API+'/ticket/'):
   if not re.fullmatch(re.escape(DATASET_API)+r'/ticket/(?:[1-9][0-9]*|sha256(?::|%3A)[0-9a-f]{64})\?sha256=[0-9a-f]{64}(?:&path=[^&]+)?(?:&generation=[a-f0-9]{32})?',path):raise ValueError('Invalid authenticated ticket route')
  elif match:
   identity=match[2] if match[1]=='coverage-downloads' else 'sha256:'+match[2]
   path=DATASET_API+'/ticket/'+urllib.parse.quote(identity,safe='')+'?sha256='+match[3]+('&path='+match[4]if match[4]else'')+('&generation='+match[5]if match[5]else'')
  else:path=DATASET_API+'/asset?path='+urllib.parse.quote(path,safe='')+('&generation='+generation if generation else'')
 headers={'User-Agent':USER_AGENT}
 if API_TOKEN:headers['Authorization']='Bearer '+API_TOKEN
 return urllib.request.Request(base+path,headers=headers)

class NoMediaRedirects(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,request,fp,code,msg,headers,newurl):
  raise ValueError('Unexpected download redirect refused')

def ticket(base,o):
 """The access server's verified R2 capability for one output of DELIVERY_GENERATION."""
 route=o.get('file_url','/api/coverage-downloads/file/'+str(o['id']))
 ticket_path=route.replace('/file/','/ticket/')+'?sha256='+o['sha256']
 if o.get('ticket_presentation_path'):ticket_path+='&path='+urllib.parse.quote(o['path'],safe='')
 if DELIVERY_GENERATION:ticket_path+='&generation='+DELIVERY_GENERATION
 opener=urllib.request.build_opener(NoMediaRedirects())
 try:response=opener.open(access_request(base,ticket_path),timeout=90)
 except urllib.error.HTTPError as e:
  if e.code==410 and DELIVERY_GENERATION:e.close();raise GenerationRetired(DELIVERY_GENERATION) from None
  raise
 with response:
  if response.status!=200:raise ValueError('Download ticket unavailable')
  raw=response.read(16385)
  if len(raw)>16384:raise ValueError('Invalid download ticket')
  t=json.loads(raw)
 u=urllib.parse.urlsplit(t.get('url',''));query=urllib.parse.parse_qs(u.query)
 host=t.get('storage_host','')
 if u.scheme!='https' or u.username or u.password or u.fragment or u.port not in (None,443) or u.hostname!=host or not re.fullmatch('[0-9a-f]{32}\\.r2\\.cloudflarestorage\\.com',host):raise ValueError('Unapproved media host')
 if t.get('sha256')!=o['sha256'] or t.get('size_bytes')!=o['size_bytes'] or t.get('filename')!=PurePosixPath(o['path']).name or t.get('transport')!='r2-direct':raise ValueError('Download ticket identity differs')
 if hashlib.sha256(u.path.encode()).hexdigest()!=t.get('object_path_sha256'):raise ValueError('Download object path differs')
 if t.get('ttl_seconds') not in (300,600) or query.get('X-Amz-Expires')!=[str(t.get('ttl_seconds'))] or query.get('X-Amz-SignedHeaders')!=['host;if-match'] or query.get('X-Amz-Algorithm')!=['AWS4-HMAC-SHA256'] or not re.fullmatch('[0-9a-f]{64}',(query.get('X-Amz-Signature') or [''])[0]):raise ValueError('Invalid download capability')
 if type(t.get('expires_at')) not in (float,int) or not time.time()+5<t['expires_at']<=time.time()+610:raise ValueError('Expired download ticket; retry')
 etag=t.get('etag')
 if not isinstance(etag,str) or not re.fullmatch(r'"[0-9a-f]{32}(?:-[0-9]+)?"',etag) or t.get('required_headers')!={'If-Match':etag}:raise ValueError('Download condition differs')
 return t

def storage(t,o,start):
 """Open the ticket's R2 object from byte start, checked against the output's identity."""
 etag=t['etag'];opener=urllib.request.build_opener(NoMediaRedirects())
 request=urllib.request.Request(t['url'],headers={'Range':'bytes=%d-'%start,'If-Match':etag,'User-Agent':USER_AGENT})
 try:response=opener.open(request,timeout=90)
 except urllib.error.HTTPError as e:
  code=e.code;e.close()
  if code in (401,403,429,500,502,503,504):raise urllib.error.URLError('Temporary R2 transfer failure; renew ticket') from None
  raise ValueError('R2 transfer rejected (%d); output withheld'%code) from None
 except urllib.error.URLError:raise urllib.error.URLError('R2 transfer unavailable; retry') from None
 if response.headers.get('ETag')!=etag:
  response.close();raise ValueError('R2 object identity changed')
 if response.headers.get('Content-Length')!=str(o['size_bytes']-start):
  response.close();raise ValueError('R2 range size differs')
 return response

def direct_request(base,o,start):return storage(ticket(base,o),o,start)

def fetch(base,o,root):
 final=destination(root,o['path']);partial=final.with_name('.output_'+o['sha256']+'.partial')
 size=o['size_bytes'];sha=o['sha256']
 if final.exists() or final.is_symlink():
  if final.is_symlink() or not final.is_file() or final.stat().st_size!=size or digest(final)!=sha:raise ValueError('Existing file differs; left untouched: '+str(final))
  return 'verified existing'
 if partial.is_symlink() or (partial.exists() and not partial.is_file()):raise ValueError('Non-regular partial refused')
 start=partial.stat().st_size if partial.exists() else 0
 if start>size:raise ValueError('Partial exceeds expected size; left untouched')
 if start<size:
  with direct_request(base,o,start) as response:
   want='bytes %d-%d/%d'%(start,size-1,size)
   if response.status!=206 or response.headers.get('Content-Range')!=want:raise ValueError('Server did not honor exact resume range')
   flags=os.O_WRONLY|os.O_APPEND|os.O_CREAT|getattr(os,'O_NOFOLLOW',0)
   fd=os.open(partial,flags,0o600)
   with os.fdopen(fd,'ab') as f:
    if os.fstat(f.fileno()).st_size!=start:raise ValueError('Partial changed during resume')
    for block in iter(lambda:response.read(256*1024),b''):f.write(block)
    f.flush();os.fsync(f.fileno())
 if partial.stat().st_size!=size or digest(partial)!=sha:raise ValueError('Checksum mismatch; partial retained for inspection')
 os.link(partial,final);partial.unlink()
 return 'downloaded and verified'

def validate_identity(o):
 sha=o['sha256'];identity=o['id'];url=o['file_url']
 if not isinstance(sha,str) or not re.fullmatch('[0-9a-f]{64}',sha) or not isinstance(o['size_bytes'],int) or o['size_bytes']<=0:raise ValueError('Invalid output identity')
 if isinstance(identity,int) and not isinstance(identity,bool) and identity>0:
  allowed=['/api/coverage-downloads/file/'+str(identity)]
  if DATASET_API:allowed.append(DATASET_API+'/file/'+str(identity))
  if url not in allowed:raise ValueError('Original output route mismatch')
 elif identity=='sha256:'+sha:
  allowed=['/api/reviewed-repairs/file/'+sha,'/api/confident-repairs/file/'+sha]
  if DATASET_API:allowed.extend([DATASET_API+'/file/sha256:'+sha,DATASET_API+'/file/sha256%3A'+sha])
  if url not in allowed:raise ValueError('Verified replacement route mismatch')
  inputs=o.get('replacement_inputs',[])
  if len(inputs)<2 or any(not isinstance(i,int) or isinstance(i,bool) or i<=0 for i in inputs) or len(set(inputs))!=len(inputs):raise ValueError('Invalid replacement inputs')
 else:raise ValueError('Invalid output identity')

def validate_manifest(m,stream=None):
 if not isinstance(m,dict) or m.get('schema')!=2:raise ValueError('Unsupported verified manifest')
 if m.get('collection')=='reviewed-jaworzno':
  recording=m.get('recording_id',427)
  if recording!=427:raise ValueError('Reviewed manifest stream mismatch')
 elif m.get('collection')=='verified-assembled':
  recording=m.get('recording_id')
  if not isinstance(recording,int) or isinstance(recording,bool) or recording<=0:raise ValueError('Invalid manifest stream')
 else:raise ValueError('Unsupported verified manifest')
 if stream is not None and recording!=stream:raise ValueError('Manifest differs from requested stream')
 outputs=m.get('outputs')
 if not isinstance(outputs,list):raise ValueError('Missing verified output list')
 paths=set();ids=set();replacements=set()
 for o in outputs:
  if not isinstance(o,dict):raise ValueError('Invalid output entry')
  relative_path(o['path']);key=unicodedata.normalize('NFC',o['path']).casefold()
  if key in paths or o['id'] in ids:raise ValueError('Duplicate manifest identity/path')
  if o.get('recording_id',recording)!=recording or not o['path'].split('/')[0].startswith(str(recording)+'_'):raise ValueError('Output belongs to another stream')
  if m['collection']=='verified-assembled' and o.get('recording_id')!=recording:raise ValueError('Verified output stream identity missing')
  if not isinstance(o.get('available'),bool):raise ValueError('Output availability missing')
  validate_identity(o);paths.add(key);ids.add(o['id'])
 for o in outputs:
  for i in o.get('replacement_inputs',[]):
   if i in ids or i in replacements:raise ValueError('Replacement would duplicate an original source part')
   replacements.add(i)
 return recording

class NoRedirects(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,request,fp,code,msg,headers,newurl):
  raise ValueError('Download server redirected; configured same-origin routes required')

def week_outputs(m,week):
 """Select seven stream-local dates anchored to the chosen 14-day period."""
 from datetime import date,timedelta
 if week not in (1,2):raise ValueError('Week must be 1 or 2')
 def day(raw):
  if not isinstance(raw,str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}',raw):raise ValueError('Invalid stream-local date')
  return date.fromisoformat(raw)
 selected=[]
 for o in m['outputs']:
  raw=m.get('period')
  if not isinstance(raw,list) or len(raw)!=2:raise ValueError('Reviewed selected period missing')
  period={'start':raw[0],'end':raw[1]}
  start=day(period['start']);end=day(period['end'])
  if end!=start+timedelta(days=13):raise ValueError('Week export requires a complete chosen 14-day period')
  local=day(o.get('local_date'))
  if not start<=local<=end:raise ValueError('Output date lies outside its selected period')
  first=start+timedelta(days=7*(week-1))
  if first<=local<first+timedelta(days=7):selected.append(o)
 return selected

def file_identity(o):
 """What one output delivers: its access identity, relative name, size, checksum and availability."""
 return (o['id'],o['file_url'],bool(o.get('ticket_presentation_path')),o['path'],o['size_bytes'],o['sha256'],o['available'])

def load_manifest(base,path,stream,week,generation):
 """A verified manifest (of generation, or the current one), its outputs limited to week; returns (stream, manifest)."""
 with urllib.request.urlopen(access_request(base,path,generation),timeout=90) as r:m=json.load(r)
 recording=validate_manifest(m,stream);published=m.get('generation')
 if published is not None and (not isinstance(published,str) or not re.fullmatch('[a-f0-9]{32}',published) or generation and published!=generation or any(o.get('delivery_generation',published)!=published for o in m['outputs'])):raise ValueError('Invalid published dataset generation')
 if week:m=dict(m,outputs=week_outputs(m,week),selected_week=week)
 return recording,m

def follow(base,path,stream,week,outputs):
 """After a 410: the current generation's manifest when it delivers exactly the same files, else GenerationChanged."""
 retired=DELIVERY_GENERATION
 for attempt in range(4):
  try:m=load_manifest(base,path,stream,week,None)[1];break
  except (urllib.error.URLError,TimeoutError,ConnectionError):
   if attempt==3:raise
   time.sleep(2**attempt)
 current=m.get('generation')
 if current is None or current==retired:raise ValueError('generation '+retired+' was refused as retired but is still served')
 before={o['path']:file_identity(o) for o in outputs};after={o['path']:file_identity(o) for o in m['outputs']}
 if before!=after:
  added=sorted(after.keys()-before.keys());removed=sorted(before.keys()-after.keys());changed=sorted(p for p in before.keys()&after.keys() if before[p]!=after[p])
  lines=['Stopped: dataset generation %s was retired, and the current generation %s changes this download: %d files added, %d removed, %d changed.'%(retired,current,len(added),len(removed),len(changed))]
  for kind,paths in (('added',added),('removed',removed),('changed',changed)):
   lines+=['  %s: %s'%(kind,p) for p in paths[:5]]+(['  ...and %d more %s'%(len(paths)-5,kind)] if len(paths)>5 else [])
  lines.append('Every file in this folder is from %s and verified; nothing from %s was downloaded. %s The fresh command keeps unchanged files and refuses any that differ.'%(retired,current,FRESH))
  raise GenerationChanged('\n'.join(lines))
 return m

def main():
 global DATASET_API,API_TOKEN,DELIVERY_GENERATION
 p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
 p.add_argument('--generation',help='Resume an exact previously published public dataset generation (32 lowercase hex characters)')
 p.add_argument('--manifest',help='Fixed verified manifest path on the configured server; omit for the default Jaworzno manifest')
 p.add_argument('--stream',required=True,type=int,help='Stream ID for its current verified assembled delivery')
 p.add_argument('--scope',choices=('best','available','audited'),default='available',help='Selected delivery period; default available; use best for maximum capture or audited for the original audit')
 p.add_argument('--week',type=int,choices=(1,2),help='Selected period week: 1=days 1–7, 2=days 8–14; omit for all 14 days')
 p.add_argument('--jobs',type=int,choices=range(1,5),default=2,help='Parallel transfers, 1–4; default 2 keeps review playback responsive');p.add_argument('--destination',required=True,type=Path);p.add_argument('--dataset-api',choices=['/api/v1/account/global-street-scores-delivery','/api/v1/global-street-scores-delivery'],default='/api/v1/global-street-scores-delivery',help='Dataset access API prefix');p.add_argument('--api-key-env',help='Environment variable containing your existing account API credential; sent only to the access server');p.add_argument('--server',default=DEFAULT,help='Manifest/access server; video bytes download directly from R2');a=p.parse_args()
 base=a.server.rstrip('/')
 DATASET_API=a.dataset_api;API_TOKEN=None;DELIVERY_GENERATION=a.generation
 if DELIVERY_GENERATION and (not re.fullmatch("[a-f0-9]{32}",DELIVERY_GENERATION)or DATASET_API!="/api/v1/global-street-scores-delivery"):p.error("--generation requires the public dataset API and its exact32-hex published generation")
 if a.api_key_env:
  if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',a.api_key_env):p.error('Invalid credential variable name')
  API_TOKEN=os.environ.get(a.api_key_env)
  if not API_TOKEN or any(ord(c)<33 or ord(c)>126 for c in API_TOKEN):p.error('Existing account credential unavailable')
  if not base.startswith('https://'):p.error('Account access requires HTTPS')
 if DATASET_API=='/api/v1/account/global-street-scores-delivery' and not API_TOKEN:p.error('--dataset-api requires --api-key-env for an existing account credential')
 origin=urllib.parse.urlsplit(base)
 if origin.username or origin.password or not origin.hostname:p.error('Server must be an origin without embedded credentials')
 if not re.fullmatch(r'https?://[^/?#]+',base):p.error('Server must be an origin URL')
 if a.stream is not None and a.stream<=0:p.error('Stream ID must be positive')
 if a.stream is not None and a.manifest is not None:p.error('Choose --stream or --manifest')
 if a.stream is not None:a.manifest='/api/coverage-delivery/manifest?stream='+str(a.stream)+'&scope='+a.scope
 else:
  a.manifest=a.manifest or '/vid/jaworzno-reviewed-repairs-20261009/manifest.json'
  if not re.fullmatch(r'/vid/[A-Za-z0-9_-]+/manifest\.json',a.manifest):p.error('Manifest must be a fixed relative review path')
 target=a.destination.expanduser()
 if target.is_symlink():p.error('Destination cannot be a symlink')
 target.mkdir(parents=True,exist_ok=True);root=target.resolve()
 urllib.request.install_opener(urllib.request.build_opener(NoRedirects()))
 try:scope,m=load_manifest(base,a.manifest,a.stream,a.week,DELIVERY_GENERATION)
 except urllib.error.HTTPError as e:
  if e.code==410 and DELIVERY_GENERATION:e.close();raise SystemExit('Dataset generation '+DELIVERY_GENERATION+' is retired. '+FRESH) from None
  raise
 generation=m.get('generation')
 if generation:
  print('Dataset generation '+generation+'. To resume this exact export, add --generation '+generation,flush=True)
  DELIVERY_GENERATION=generation  # only public manifests carry one
 # Small receipt manifest enables later audit; media is never archived/duplicated.
 receipt=root/('stitching-'+str(scope)+'-reviewed'+('-week'+str(a.week) if a.week else '')+'.manifest.json')
 save_receipt(receipt,m)
 outputs=m['outputs'];total=len(outputs);failures=[]
 def transfer(n,o):
  if not o['available']:
   print('[%d/%d] Unavailable output %s'%(n,total,o['id']));return o['id']
  for attempt in range(4):
   try:
    status=fetch(base,o,root);print('[%d/%d] %s: %s'%(n,total,status,o['path']));return None
   except GenerationRetired:return RETIRED
   except (urllib.error.URLError,TimeoutError,ConnectionError):
    if attempt<3:time.sleep(2**attempt);continue
    print('Transfer failed for output '+str(o['id'])+'; rerun to resume',file=sys.stderr);return o['id']
   except (ValueError,OSError) as e:
    print(str(e),file=sys.stderr);return o['id']
 # Bound pending work as well as active sockets for the 70k-file export. After a
 # 410, finish what is in flight, then switch generations once or stop.
 work=collections.deque(enumerate(outputs,1));pending={};retired=[];stopped=None
 with ThreadPoolExecutor(max_workers=a.jobs) as pool:
  while not stopped:
   while work and not retired and len(pending)<a.jobs:
    item=work.popleft();pending[pool.submit(transfer,*item)]=item
   if pending:
    done=next(as_completed(pending));item=pending.pop(done);result=done.result()
    if result is RETIRED:retired.append(item)
    elif result is not None:failures.append(result)
    continue
   if not retired:break
   old=DELIVERY_GENERATION
   try:m=follow(base,a.manifest,a.stream,a.week,outputs)
   except GenerationChanged as e:stopped=str(e)
   except (urllib.error.URLError,TimeoutError,ConnectionError,ValueError) as e:stopped='Stopped: dataset generation '+old+' was retired and the current generation could not be checked ('+str(e)+'). '+FRESH
   else:
    DELIVERY_GENERATION=m['generation'];save_receipt(receipt,m)
    print('Dataset generation %s was retired; generation %s lists the same %d files, so this download continues on it. Files already downloaded are kept.'%(old,DELIVERY_GENERATION,total),flush=True)
    work.extendleft(reversed(retired));retired.clear()
 if stopped:
  print(stopped,file=sys.stderr)
  print('%d outputs complete; %d unavailable or failed; %d not started.'%(total-len(failures)-len(work)-len(retired),len(failures),len(work)+len(retired)))
  return True
 print('%d outputs complete; %d unavailable or failed. Rerun to resume.'%(total-len(failures),len(failures)))
 return bool(failures)
if __name__=='__main__':
 try:sys.exit(main())
 except KeyboardInterrupt:print('\nStopped. Rerun to resume partial files.',file=sys.stderr);sys.exit(130)
