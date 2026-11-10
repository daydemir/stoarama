"""Read-only, same-tailnet delivery for the exact audited 49-stream cohort.

Integrate get/head before the existing review handler routes. The private index
must live outside static ROOT. Never publish object keys or capabilities in catalogs. Mint short-lived GET tickets on demand.
"""
import json, os, re, shlex, subprocess, threading, urllib.request, urllib.error
from pathlib import Path
from urllib.parse import quote
from datetime import date,timedelta

BASE='/api/coverage-downloads/'
INDEX=Path(os.environ.get('COVERAGE_DOWNLOAD_INDEX','/Users/jelly/.stoarama/seam-review/private/coverage-download-index.json'))
CREDS=Path('/Users/jelly/Build/stoarama/local/render.env')
AWS='/Users/jelly/.local/bin/aws'
_LIMIT=threading.BoundedSemaphore(4)
_LOCK=threading.Lock()
_DATA=None
_MTIME=None

def data():
 global _DATA,_MTIME
 with _LOCK:
  stamp=INDEX.stat().st_mtime_ns
  if _DATA is None or _MTIME!=stamp:
   d=json.loads(INDEX.read_text());d['by_id']={str(o['id']):o for o in d['outputs']};_DATA=d;_MTIME=stamp
 return _DATA

def public(o):
 return {k:v for k,v in o.items() if k not in ('object_key','etag','nas_relative_path','r2_verified_at','hour_id')}

def periods(d,collection,stream='all'):
 if collection=='extra':return {}
 result={}
 for s in d['streams']:
  if stream!='all' and str(s['id'])!=stream:continue
  start=s[collection+'_start'] if collection in ('best','available') else s['start'];end=s[collection+'_end'] if collection in ('best','available') else s['end']
  if (date.fromisoformat(end)-date.fromisoformat(start)).days!=13:raise ValueError('Invalid selected period')
  result[str(s['id'])]={'start':start,'end':end}
 return result

def weekly_outputs(outputs,chosen,week):
 if type(week) is not int or week not in (1,2):raise ValueError('Week must be 1 or 2')
 out=[]
 for o in outputs:
  period=chosen[str(o['recording_id'])];anchor=date.fromisoformat(period['start']);end=date.fromisoformat(period['end'])
  start=anchor+timedelta(days=(week-1)*7)
  day=date.fromisoformat(o['local_date'])
  if not anchor<=day<=end:raise ValueError('Output outside selected 14-day period')
  if start<=day<start+timedelta(days=7):out.append(o)
 return out

def weekly_catalog(d):
 # Cache the small projection with the immutable-index cache. No media reads.
 if '_weekly_exports' in d:return d['_weekly_exports']
 summary={str(s['id']):{} for s in d['streams']}
 for collection in ('audited','best')+(('available',) if all('available_start' in s for s in d['streams']) else ()):
  chosen=periods(d,collection)
  for sid,p in chosen.items():
   start=date.fromisoformat(p['start']);weeks=[]
   for w in (1,2):
    a=start+timedelta(days=(w-1)*7);weeks.append({'number':w,'start':a.isoformat(),'end':(a+timedelta(days=6)).isoformat(),'parts':0,'available':0,'size_bytes':0,'sample_path':None,'sample_local_date':None})
   summary[sid][collection]={'period':p,'weeks':weeks}
  for o in d['outputs']:
   if collection not in o.get('collections',[o['scope']]):continue
   sid=str(o['recording_id']);delta=(date.fromisoformat(o['local_date'])-date.fromisoformat(chosen[sid]['start'])).days
   if not 0<=delta<14:raise ValueError('Output outside chosen period')
   w=summary[sid][collection]['weeks'][delta//7];w['parts']+=1;w['available']+=int(o['available']);w['size_bytes']+=o['size_bytes']
   if w['sample_path'] is None and o['available']:w['sample_path']=o['path'];w['sample_local_date']=o['local_date']
 d['_weekly_exports']=summary;return summary

def send(h,status,body,ctype='application/json',filename=None,head=False):
 raw=body if isinstance(body,bytes) else json.dumps(body,ensure_ascii=False).encode()
 h.send_response(status);h.send_header('Content-Type',ctype);h.send_header('Content-Length',str(len(raw)));h.send_header('Cache-Control','no-store');h.send_header('X-Content-Type-Options','nosniff')
 if filename:h.send_header('Content-Disposition',disposition(filename,True))
 h.end_headers()
 if not head:h.wfile.write(raw)
 return True

def disposition(name,download):
 # RFC 6266 fallback has no header metacharacters; UTF-8 is percent-encoded.
 ascii_name=re.sub(r'[^A-Za-z0-9._-]','_',name)
 return ('attachment' if download else 'inline')+'; filename="'+ascii_name+'"; filename*=UTF-8\'\''+quote(name,safe='')

def aws(args):
 # The existing credential file stays on Jelly and is consumed by its existing
 # AWS CLI. stdout may contain a private URL, so it must never be logged.
 cmd='set -a; . '+shlex.quote(str(CREDS))+'; set +a; export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" AWS_DEFAULT_REGION="${R2_REGION:-auto}" AWS_PAGER=""; exec '+shlex.quote(AWS)+' --endpoint-url "https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com" --region "${R2_REGION:-auto}" '+args
 r=subprocess.run(['/bin/sh','-c',cmd],capture_output=True,text=True,timeout=40)
 if r.returncode:raise RuntimeError('storage_unavailable')
 return r.stdout

def storage(o,head_only=False):
 head=json.loads(aws('s3api head-object --bucket "$R2_BUCKET" --key '+shlex.quote(o['object_key'])+' --output json'))
 if head.get('ContentLength')!=o['size_bytes'] or head.get('ETag')!=o['etag']:raise ValueError('object_changed')
 if head_only:return None
 url=aws('s3 presign "s3://${R2_BUCKET}/"'+shlex.quote(o['object_key'])+' --expires-in 3600').strip()
 if not url.startswith('https://'):raise RuntimeError('storage_unavailable')
 return url

def ticket(h,o,q,head=False,browser=False):
 # Existing private service access remains the authorization boundary. Cross-site
 # requests cannot mint capabilities; terminal clients have no Origin header.
 origin=h.headers.get('Origin');host=h.headers.get('Host','')
 if origin and origin not in ('https://'+host,('http://'+host if host.split(':')[0] in ('127.0.0.1','localhost','[::1]') else '')):return send(h,403,{'error':'Same-origin access required'},head=head)
 if q.get('sha256') != [o['sha256']]:return send(h,400,{'error':'Verified output SHA required'},head=head)
 if not o['available']:return send(h,404,{'error':'Verified output unavailable'},head=head)
 if 'path' in q:
  if len(q['path'])!=1:return send(h,400,{'error':'Choose one verified presentation path'},head=head)
  try:
   import coverage_growth
   o=coverage_growth.ticket_alias(o,q['path'][0])
  except (ValueError,KeyError,TypeError):return send(h,400,{'error':'Presentation path differs from verified manifest'},head=head)
 if head:return send(h,405,{'error':'Use GET for a fresh download ticket'},head=True)
 if not _LIMIT.acquire(blocking=False):return send(h,429,{'error':'Retry download ticket shortly'})
 try:
  storage(o,True)
  signer=Path(__file__).parent/'r2_ticket_signer.py'
  cmd='set -a; . '+shlex.quote(str(CREDS))+'; set +a; exec python3 '+shlex.quote(str(signer))
  result=subprocess.run(['/bin/sh','-c',cmd],input=json.dumps({'output':o,'conditional':not browser}),capture_output=True,text=True,timeout=15)
  if result.returncode:raise RuntimeError('Signing unavailable')
  d=json.loads(result.stdout)
  if browser:
   h.send_response(302);h.send_header('Location',d['url']);h.send_header('Content-Length','0');h.send_header('Cache-Control','private, no-store');h.send_header('Referrer-Policy','no-referrer');h.end_headers();return True
  return send(h,200,d)
 except ValueError:return send(h,409,{'error':'R2 output identity changed; download withheld'})
 except Exception:return send(h,503,{'error':'R2 ticket unavailable; retry shortly'})
 finally:_LIMIT.release()

def byte_range(raw,size):
 if raw is None:return None
 m=re.fullmatch(r'bytes=(\d*)-(\d*)',raw)
 if not m or not any(m.groups()):raise ValueError('invalid_range')
 a,b=m.groups()
 if not a:
  n=int(b)
  if n<=0:raise ValueError('invalid_range')
  return (max(0,size-n),size-1)
 start=int(a);end=int(b) if b else size-1
 if start>=size or end<start:raise ValueError('invalid_range')
 return start,min(end,size-1)

def file(h,o,q,head=False):
 if q.get('download')==['1'] and q.get('direct')==['1']:
  return ticket(h,o,dict(q,sha256=[o['sha256']]),head,browser=True)
 if not o['available']:return send(h,404,{'error':o.get('unavailable_reason','R2 object unavailable')},head=head)
 try:span=byte_range(h.headers.get('Range'),o['size_bytes'])
 except ValueError:
  h.send_response(416);h.send_header('Content-Range','bytes */'+str(o['size_bytes']));h.send_header('Content-Length','0');h.end_headers();return True
 if not _LIMIT.acquire(blocking=False):return send(h,429,{'error':'Four transfers are active. Retry shortly.'},head=head)
 sent=False;old_timeout=h.connection.gettimeout();h.connection.settimeout(45)
 try:
  url=storage(o,head)
  if head:
   h.send_response(200);h.send_header('Content-Type','video/mp4');h.send_header('Content-Length',str(o['size_bytes']));h.send_header('Accept-Ranges','bytes');h.send_header('ETag',o['etag']);h.send_header('Cache-Control','private, no-store');h.send_header('Content-Disposition',disposition(Path(o['path']).name,q.get('download')==['1']));h.send_header('X-Content-Type-Options','nosniff');h.end_headers();return True
  headers={'If-Match':o['etag']}
  # Client identity constraints may tighten ours, never replace it.
  client_match=h.headers.get('If-Match')
  if client_match and client_match not in (o['etag'],'*'):return send(h,412,{'error':'Output identity differs'},head=head)
  if span:headers['Range']='bytes=%d-%d'%span
  with urllib.request.urlopen(urllib.request.Request(url,headers=headers,method='HEAD' if head else 'GET'),timeout=40) as remote:
   expected=206 if span and not head else 200
   if remote.status!=expected:raise RuntimeError('storage_response_invalid')
   etag=remote.headers.get('ETag')
   if etag!=o['etag']:raise ValueError('object_changed')
   if span and not head and remote.headers.get('Content-Range')!='bytes %d-%d/%d'%(*span,o['size_bytes']):raise RuntimeError('storage_response_invalid')
   size=span[1]-span[0]+1 if span and not head else o['size_bytes']
   if int(remote.headers.get('Content-Length','-1'))!=size:raise RuntimeError('storage_response_invalid')
   h.send_response(expected);h.send_header('Content-Type','video/mp4');h.send_header('Content-Length',str(size));h.send_header('Accept-Ranges','bytes');h.send_header('ETag',etag);h.send_header('Cache-Control','private, no-store');h.send_header('X-Content-Type-Options','nosniff');h.send_header('Content-Disposition',disposition(Path(o['path']).name,q.get('download')==['1']))
   if span and not head:h.send_header('Content-Range',remote.headers['Content-Range'])
   h.end_headers();sent=True
   if not head:
    while True:
     block=remote.read(256*1024)
     if not block:break
     h.wfile.write(block)
   return True
 except (BrokenPipeError,ConnectionResetError):return True
 except ValueError:return send(h,409,{'error':'R2 object identity changed; transfer withheld'},head=head)
 except urllib.error.HTTPError as e:return send(h,404 if e.code==404 else 409 if e.code==412 else 503,{'error':'R2 transfer unavailable; retry or inspect the manifest'},head=head)
 except Exception:
  if sent:h.close_connection=True;return True
  return send(h,503,{'error':'R2 transfer unavailable; retry shortly'},head=head)
 finally:
  h.connection.settimeout(old_timeout);_LIMIT.release()

def route(h,u,q,head=False):
 if not u.path.startswith(BASE):return False
 try:d=data()
 except Exception:return send(h,503,{'error':'Download verification index unavailable'},head=head)
 path=u.path[len(BASE):]
 if path=='catalog':
  result={k:d[k] for k in ('verified_at','verification','naming','streams')}
  if (q.get('weekly') or ['0'])[0]=='1':
   try:
    exports=weekly_catalog(d);result['streams']=[dict(s,weekly_exports=exports[str(s['id'])]) for s in d['streams']]
   except (ValueError,KeyError,TypeError):return send(h,503,{'error':'Selected-week metadata unavailable'},head=head)
  return send(h,200,result,head=head)
 if path=='manifest':
  scope=(q.get('stream') or ['all'])[0]
  if scope!='all' and not any(str(s['id'])==scope for s in d['streams']):return send(h,404,{'error':'Unknown cohort stream'},head=head)
  collection=(q.get('scope') or ['audited'])[0]
  if collection not in ('audited','extra','best','available'):return send(h,400,{'error':'Unknown collection'},head=head)
  week=None
  if 'week' in q:
   if collection=='extra' or len(q['week'])!=1 or q['week'][0] not in ('1','2'):return send(h,400,{'error':'Week must be 1 or 2 within a selected 14-day collection'},head=head)
   week=int(q['week'][0])
  out=[public(o) for o in d['outputs'] if collection in o.get('collections',[o['scope']]) and (scope=='all' or str(o['recording_id'])==scope)]
  try:
   chosen=periods(d,collection,scope)
   if week is not None:out=weekly_outputs(out,chosen,week)
  except (ValueError,KeyError,TypeError):return send(h,503,{'error':'Selected-period dates unavailable; no files omitted silently'},head=head)
  result={'schema':1,'base_path':BASE,'scope':scope,'collection':collection,'verified_at':d['verified_at'],'naming':d['naming'],'periods':chosen,'outputs':out}
  if week is not None:result['week']={'number':week,'definition':'Selected-period days 1–7 or 8–14; filename Week stays calendar-month week'}
  return send(h,200,result,filename='stitching-'+scope+'-'+collection+('-week'+str(week) if week else '')+'.json',head=head)
 if path=='downloader.py':return send(h,200,(Path(__file__).parent/'coverage_downloader.py').read_bytes(),'text/x-python; charset=utf-8','coverage_downloader.py',head=head)
 m=re.fullmatch(r'(file|ticket)/([1-9][0-9]{0,18})',path)
 if m:
  o=d['by_id'].get(m[2])
  return (ticket(h,o,q,head) if m[1]=='ticket' else file(h,o,q,head)) if o else send(h,404,{'error':'Unknown cohort output'},head=head)
 return send(h,404,{'error':'Unknown download route'},head=head)

def get(h,u,q):
 if u.path in ('/api/coverage-growth','/api/coverage-delivery/manifest'):
  import coverage_growth
  return coverage_growth.get(h,u,q)
 return route(h,u,q)
def head(h,u,q):return route(h,u,q,True)
