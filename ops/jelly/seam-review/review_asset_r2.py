"""Private immutable fallback for archived review bytes; configs and indexes stay local."""
import json, os, re, threading, urllib.request, urllib.error
from pathlib import Path
import coverage_downloads as delivery
INDEX=Path(os.environ.get('REVIEW_ASSET_R2_INDEX','/Users/jelly/.stoarama/seam-review/private/review-asset-r2-index.json'))
ROOT=Path(os.environ.get('REVIEW_ASSET_STATIC_ROOT','/Users/jelly/.stoarama/seam-review/app/stoarama-proof/review'))
PATH=re.compile(r'^/(?:frame-review/[a-f0-9]{64}/frame-([a-f0-9]{64})\.png|vid/terminal-heldout-\d{8}(?:-batch\d+)?-case-\d+(?:-full)?\.mp4)$')
PREFIXES=('experimental/stitching-review-asset-cache/20261009/','experimental/stitching-confident-repairs/20261009/canary-assets/')
_LOCK=threading.Lock();_DATA={};_STAMP=None

def records():
 global _DATA,_STAMP
 with _LOCK:
  stamp=INDEX.stat().st_mtime_ns
  if stamp!=_STAMP:
   v=json.loads(INDEX.read_text());assert v['version']==1
   valid={}
   for p,x in v['assets'].items():
    m=PATH.fullmatch(p);assert m and re.fullmatch('[a-f0-9]{64}',x['sha256'])
    assert not m.group(1)or m.group(1)==x['sha256']
    assert type(x['size_bytes'])is int and x['size_bytes']>0 and x['full_R2_SHA_readback_verified']is True
    assert x['object_key']in tuple(prefix+x['sha256']+Path(p).suffix for prefix in PREFIXES)
    assert isinstance(x['etag'],str)and re.fullmatch(r'"[a-zA-Z0-9-]+"',x['etag'])
    valid[p]=x
   _DATA=valid;_STAMP=stamp
  return _DATA

def available(path,expectedSHA):
 try:return records().get(path,{}).get('sha256')==expectedSHA
 except (OSError,ValueError,KeyError,AssertionError):return False

def route(h,u,q,head=False):
 p=u.path
 if not PATH.fullmatch(p)or(ROOT/p.lstrip('/')).is_file():return False
 try:o=records().get(p)
 except (OSError,ValueError,KeyError,AssertionError):return False
 if not o:return False
 try:span=delivery.byte_range(h.headers.get('Range'),o['size_bytes'])
 except ValueError:
  h.send_response(416);h.send_header('Content-Range','bytes */'+str(o['size_bytes']));h.send_header('Content-Length','0');h.end_headers();return True
 if h.headers.get('If-Match')not in (None,'*',o['etag']):return delivery.send(h,412,{'error':'Review asset identity differs'},head=head)
 if not delivery._LIMIT.acquire(blocking=False):return delivery.send(h,429,{'error':'Retry review asset shortly'},head=head)
 sent=False
 try:
  url=delivery.storage(o,head)
  ctype='image/png'if p.endswith('.png')else'video/mp4'
  def response(code,size,content_range=None):
   h.send_response(code)
   for k,v in [('Content-Type',ctype),('Content-Length',str(size)),('ETag',o['etag']),('Accept-Ranges','bytes'),('Cache-Control','private, no-store'),('X-Content-Type-Options','nosniff')]:h.send_header(k,v)
   if content_range:h.send_header('Content-Range',content_range)
   h.end_headers()
  if head:response(200,o['size_bytes']);return True
  headers={'If-Match':o['etag']}
  if span:headers['Range']='bytes=%d-%d'%span
  with urllib.request.urlopen(urllib.request.Request(url,headers=headers),timeout=40)as remote:
   status=206 if span else 200;size=span[1]-span[0]+1 if span else o['size_bytes'];cr='bytes %d-%d/%d'%(*span,o['size_bytes'])if span else None
   assert remote.status==status and remote.headers.get('ETag')==o['etag']and int(remote.headers.get('Content-Length','-1'))==size
   assert not cr or remote.headers.get('Content-Range')==cr
   response(status,size,cr);sent=True;count=0
   while True:
    b=remote.read(256*1024)
    if not b:break
    count+=len(b);assert count<=size;h.wfile.write(b)
   assert count==size
  return True
 except(BrokenPipeError,ConnectionResetError):return True
 except Exception:
  if sent:h.close_connection=True;return True
  return delivery.send(h,503,{'error':'Archived review asset temporarily unavailable'},head=head)
 finally:delivery._LIMIT.release()

def get(h,u,q):
 import review_preview_r2
 if review_preview_r2.get(h,u,q):return True
 return route(h,u,q)
def head(h,u,q):return route(h,u,q,True)
