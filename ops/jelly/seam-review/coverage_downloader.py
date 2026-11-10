#!/usr/bin/env python3
"""Download verified stitching outputs into the site's named folder tree.

Requires Python 3.9 or newer; no additional packages.
Save this script, then run the command in Terminal.
python3 coverage_downloader.py --stream 444 --destination ./stitched
python3 coverage_downloader.py --all --destination ./stitched
Rerun the same command to resume. Existing files are never overwritten.
"""
import argparse,hashlib,json,os,re,sys,time,urllib.request,urllib.error,urllib.parse,unicodedata
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path,PurePosixPath
DEFAULT='https://jellys-mac-mini.taildfdcc0.ts.net:8476'
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

DATASET_API=None
API_TOKEN=None

def access_request(base,path):
 if DATASET_API:
  match=re.fullmatch(r'/api/(coverage-downloads|confident-repairs|reviewed-repairs)/ticket/([^/?]+)\?sha256=([0-9a-f]{64})',path)
  if path.startswith(DATASET_API+'/ticket/'):
   if not re.fullmatch(re.escape(DATASET_API)+r'/ticket/(?:[1-9][0-9]*|sha256(?::|%3A)[0-9a-f]{64})\?sha256=[0-9a-f]{64}',path):raise ValueError('Invalid authenticated ticket route')
  elif match:
   identity=match[2] if match[1]=='coverage-downloads' else 'sha256:'+match[2]
   path=DATASET_API+'/ticket/'+urllib.parse.quote(identity,safe='')+'?sha256='+match[3]
  else:path=DATASET_API+'/asset?path='+urllib.parse.quote(path,safe='')
 headers={'User-Agent':USER_AGENT}
 if API_TOKEN:headers['Authorization']='Bearer '+API_TOKEN
 return urllib.request.Request(base+path,headers=headers)

class NoMediaRedirects(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,request,fp,code,msg,headers,newurl):
  raise ValueError('Unexpected download redirect refused')

def direct_request(base,o,start):
 route=o.get('file_url','/api/coverage-downloads/file/'+str(o['id']))
 ticket_path=route.replace('/file/','/ticket/')+'?sha256='+o['sha256']
 opener=urllib.request.build_opener(NoMediaRedirects())
 with opener.open(access_request(base,ticket_path),timeout=90) as response:
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

def fetch(base,o,root):
 final=destination(root,o['path']);partial=final.with_name('.output_'+str(o['id'])+'.partial')
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

def week_outputs(m,week):
 """Select seven stream-local dates anchored to the chosen 14-day period."""
 from datetime import date,timedelta
 if week not in (1,2):raise ValueError('Week must be 1 or 2')
 def day(raw):
  if not isinstance(raw,str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}',raw):raise ValueError('Invalid stream-local date')
  return date.fromisoformat(raw)
 selected=[]
 for o in m['outputs']:
  recording=o.get('recording_id')
  if not isinstance(recording,int) or isinstance(recording,bool) or recording<=0:raise ValueError('Invalid stream identity for weekly export')
  period=m.get('periods',{}).get(str(recording))
  if not isinstance(period,dict):raise ValueError('Selected period missing for stream '+str(recording))
  start=day(period['start']);end=day(period['end'])
  if end!=start+timedelta(days=13):raise ValueError('Week export requires a complete chosen 14-day period')
  local=day(o.get('local_date'))
  if not start<=local<=end:raise ValueError('Output date lies outside its selected period')
  first=start+timedelta(days=7*(week-1))
  if first<=local<first+timedelta(days=7):selected.append(o)
 return selected

def main():
 global DATASET_API,API_TOKEN
 p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
 scope=p.add_mutually_exclusive_group(required=True);scope.add_argument('--stream',type=int);scope.add_argument('--all',action='store_true')
 p.add_argument('--week',type=int,choices=(1,2),help='Selected period week: 1=days 1–7, 2=days 8–14; omit for all 14 days')
 p.add_argument('--jobs',type=int,choices=range(1,5),default=2,help='Parallel transfers, 1–4; default 2 keeps review playback responsive');p.add_argument('--scope',choices=['audited','extra','best','available'],default='audited');p.add_argument('--destination',required=True,type=Path);p.add_argument('--dataset-api',choices=['/api/v1/account/global-street-scores-delivery'],help='Authenticated dataset API prefix');p.add_argument('--api-key-env',help='Environment variable containing your existing account API credential; sent only to the access server');p.add_argument('--server',default=DEFAULT,help='Manifest/access server; video bytes download directly from R2');a=p.parse_args()
 if a.week and a.scope=='extra':p.error('--week is available for audited and best 14-day periods only')
 base=a.server.rstrip('/')
 DATASET_API=a.dataset_api;API_TOKEN=None
 if a.api_key_env:
  if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',a.api_key_env):p.error('Invalid credential variable name')
  API_TOKEN=os.environ.get(a.api_key_env)
  if not API_TOKEN or any(ord(c)<33 or ord(c)>126 for c in API_TOKEN):p.error('Existing account credential unavailable')
  if not base.startswith('https://'):p.error('Account access requires HTTPS')
 if DATASET_API and not API_TOKEN:p.error('--dataset-api requires --api-key-env for an existing account credential')
 origin=urllib.parse.urlsplit(base)
 if origin.username or origin.password or not origin.hostname:p.error('Server must be an origin without embedded credentials')
 if not re.fullmatch(r'https?://[^/?#]+',base):p.error('Server must be an origin URL')
 if a.stream is not None and a.stream<=0:p.error('Stream ID must be positive')
 target=a.destination.expanduser()
 if target.is_symlink():p.error('Destination cannot be a symlink')
 target.mkdir(parents=True,exist_ok=True);root=target.resolve()
 urllib.request.install_opener(urllib.request.build_opener(NoMediaRedirects()))
 scope=str(a.stream) if a.stream is not None else 'all'
 with urllib.request.urlopen(access_request(base,'/api/coverage-downloads/manifest?stream='+scope+'&scope='+a.scope+('&week='+str(a.week) if a.week else '')),timeout=90) as r:m=json.load(r)
 if m.get('collection')!=a.scope:raise ValueError('Manifest collection differs from requested collection')
 if m.get('schema')!=1:raise ValueError('Unsupported manifest')
 paths=set();ids=set()
 for o in m['outputs']:
  relative_path(o['path']);key=unicodedata.normalize('NFC',o['path']).casefold()
  if key in paths or o['id'] in ids:raise ValueError('Duplicate manifest identity/path')
  if not isinstance(o['id'],int) or o['id']<=0 or not re.fullmatch('[0-9a-f]{64}',o['sha256']) or o['size_bytes']<=0:raise ValueError('Invalid output identity')
  paths.add(key);ids.add(o['id'])
 if a.week:
  m=dict(m,outputs=week_outputs(m,a.week),selected_week=a.week)
  ids={o['id'] for o in m['outputs']}
 # Small receipt manifest enables later audit; media is never archived/duplicated.
 receipt=root/('stitching-'+scope+'-'+a.scope+('-week'+str(a.week) if a.week else '')+'.manifest.json')
 save_receipt(receipt,m)
 failures=[]
 def transfer(n,o):
  if not o['available']:
   print('[%d/%d] Unavailable output %s'%(n,len(ids),o['id']));return o['id']
  for attempt in range(4):
   try:
    status=fetch(base,o,root);print('[%d/%d] %s: %s'%(n,len(ids),status,o['path']));return None
   except (urllib.error.URLError,TimeoutError,ConnectionError):
    if attempt<3:time.sleep(2**attempt);continue
    print('Transfer failed for output '+str(o['id'])+'; rerun to resume',file=sys.stderr);return o['id']
   except (ValueError,OSError) as e:
    print(str(e),file=sys.stderr);return o['id']
 with ThreadPoolExecutor(max_workers=a.jobs) as pool:
  # Bound pending work as well as active sockets for the 70k-file export.
  pending={};items=iter(enumerate(m['outputs'],1))
  for _ in range(a.jobs):
   item=next(items,None)
   if item:pending[pool.submit(transfer,*item)]=item[1]['id']
  while pending:
   done=next(as_completed(pending));result=done.result();del pending[done]
   if result is not None:failures.append(result)
   item=next(items,None)
   if item:pending[pool.submit(transfer,*item)]=item[1]['id']
 print('%d outputs complete; %d unavailable or failed. Rerun to resume.'%(len(ids)-len(failures),len(failures)))
 return bool(failures)
if __name__=='__main__':
 try:sys.exit(main())
 except KeyboardInterrupt:print('\nStopped. Rerun to resume partial files.',file=sys.stderr);sys.exit(130)
