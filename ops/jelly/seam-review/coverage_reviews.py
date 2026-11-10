"""Manual judgments for immutable indexed outputs; separate from seam votes."""
import json, os, re, sqlite3, time
from pathlib import Path
from urllib.parse import urlparse
import coverage_downloads as delivery

ROUTE='/api/coverage-reviews'
DB=Path(os.environ.get('COVERAGE_REVIEW_DB','/Users/jelly/.stoarama/seam-review/private/coverage-reviews.sqlite3'))
TAILNET='jellys-mac-mini.taildfdcc0.ts.net:8476'
SCOPES={'best','audited','extra','available'}

def send(h,code,value):
 return delivery.send(h,code,value)

def number(value):
 if isinstance(value,bool) or not re.fullmatch(r'[1-9][0-9]{0,15}',str(value)):raise ValueError('Invalid output or stream ID')
 return int(value)

def query(q,k,default=None):
 values=q.get(k,[default])
 if len(values)!=1:raise ValueError('Repeated query parameter')
 return values[0]

def scope(value):
 if value not in SCOPES:raise ValueError('Invalid collection')
 return value

def output(i,collection):
 o=delivery.data()['by_id'].get(str(number(i)))
 if not o or collection not in o.get('collections',[o.get('scope','audited')]):raise LookupError('Output not in selected collection')
 return o

def connect(write=False):
 if DB.is_symlink():raise ValueError('Review storage is unavailable')
 if write:
  DB.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
  fd=os.open(DB,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600);os.close(fd)
  os.chmod(DB,0o600)
  c=sqlite3.connect(DB,timeout=5)
  c.execute('CREATE TABLE IF NOT EXISTS reviews (output_id INTEGER NOT NULL, sha256 TEXT NOT NULL, verdict TEXT NOT NULL, note TEXT NOT NULL, updated_at REAL NOT NULL, PRIMARY KEY(output_id,sha256))')
  c.execute('CREATE TABLE IF NOT EXISTS history (event_id INTEGER PRIMARY KEY, output_id INTEGER NOT NULL, sha256 TEXT NOT NULL, verdict TEXT NOT NULL, note TEXT NOT NULL, created_at REAL NOT NULL)')
  c.commit()
 else:c=sqlite3.connect(DB.as_uri()+'?mode=ro',uri=True,timeout=5)
 c.row_factory=sqlite3.Row
 return c

def stored():
 if not DB.exists():return []
 with connect() as c:return [dict(r) for r in c.execute('SELECT * FROM reviews')]

def get(h,u,q):
 if u.path!=ROUTE:return False
 try:
  collection=scope(query(q,'scope','best'))
  rows=stored()
  if 'output' in q:
   o=output(query(q,'output'),collection)
   review=next((r for r in rows if r['output_id']==o['id'] and r['sha256']==o['sha256']),None)
   return send(h,200,{'output_id':o['id'],'sha256':o['sha256'],'recording_id':o['recording_id'],'scope':collection,'review':review})
  stream=number(query(q,'stream'))
  d=delivery.data()
  if stream not in {s['id'] for s in d['streams']}:raise LookupError('Unknown stream')
  selected={o['id']:o for o in d['outputs'] if o['recording_id']==stream and collection in o.get('collections',[o.get('scope','audited')])}
  reviews=[r for r in rows if r['output_id'] in selected and r['sha256']==selected[r['output_id']]['sha256']]
  counts={v:sum(r['verdict']==v for r in reviews) for v in ('good','issue','uncertain')};counts['unreviewed']=len(selected)-len(reviews)
  return send(h,200,{'stream_id':stream,'scope':collection,'parts':len(selected),'counts':counts,'reviews':reviews,'meaning':'Manual part judgments, not automatic perfect-stitch certification.'})
 except LookupError as e:return send(h,404,{'error':str(e)})
 except ValueError as e:return send(h,400,{'error':str(e)})
 except Exception:return send(h,503,{'error':'Manual review storage unavailable; existing reviews preserved.'})

def same_origin(h):
 raw=h.headers.get('Origin','');host=h.headers.get('Host','').lower()
 if raw=='https://'+TAILNET and host in (TAILNET,'127.0.0.1:8476','localhost:8476'):return True
 u=urlparse(raw)
 return u.scheme=='http' and u.hostname in ('127.0.0.1','localhost','::1') and u.netloc.lower()==host and not u.path and not u.query and not u.fragment

def post(h):
 if urlparse(h.path).path!=ROUTE:return False
 old_timeout=h.connection.gettimeout()
 try:
  if not same_origin(h):h.close_connection=True;return send(h,403,{'error':'Same-origin review request required'})
  if h.headers.get('Transfer-Encoding') or h.headers.get('Content-Type','').split(';')[0].strip().lower()!='application/json':h.close_connection=True;return send(h,415,{'error':'JSON request required'})
  raw=h.headers.get('Content-Length','')
  if not re.fullmatch(r'[0-9]+',raw):h.close_connection=True;return send(h,411,{'error':'Content length required'})
  n=int(raw)
  if n<2 or n>8192:h.close_connection=True;return send(h,413,{'error':'Review request too large or empty'})
  h.connection.settimeout(10)
  try:req=json.loads(h.rfile.read(n))
  except (ValueError,UnicodeDecodeError):raise ValueError('Invalid JSON')
  if not isinstance(req,dict):raise ValueError('Review must be an object')
  collection=scope(req.get('scope','best'));o=output(req.get('output_id'),collection)
  sha=req.get('sha256')
  if not isinstance(sha,str) or sha!=o['sha256']:return send(h,409,{'error':'Output identity changed; review withheld'})
  if 'stream_id' in req and number(req['stream_id'])!=o['recording_id']:raise ValueError('Stream identity differs')
  verdict=req.get('verdict');note=req.get('note','')
  if verdict not in ('good','issue','uncertain','clear'):raise ValueError('Invalid verdict')
  if not isinstance(note,str) or len(note)>1000:raise ValueError('Note must contain at most 1000 characters')
  now=time.time()
  with connect(True) as c:
   c.execute('BEGIN IMMEDIATE')
   if verdict=='clear':c.execute('DELETE FROM reviews WHERE output_id=? AND sha256=?',(o['id'],sha))
   else:c.execute('INSERT INTO reviews VALUES(?,?,?,?,?) ON CONFLICT(output_id,sha256) DO UPDATE SET verdict=excluded.verdict,note=excluded.note,updated_at=excluded.updated_at',(o['id'],sha,verdict,note,now))
   c.execute('INSERT INTO history(output_id,sha256,verdict,note,created_at) VALUES(?,?,?,?,?)',(o['id'],sha,verdict,note,now))
  return send(h,200,{'ok':True,'output_id':o['id'],'sha256':sha,'verdict':verdict})
 except LookupError as e:return send(h,404,{'error':str(e)})
 except ValueError as e:return send(h,400,{'error':str(e)})
 except Exception:h.close_connection=True;return send(h,503,{'error':'Review not saved; existing reviews preserved.'})
 finally:h.connection.settimeout(old_timeout)
