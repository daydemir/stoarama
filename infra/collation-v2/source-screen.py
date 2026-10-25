"""Bounded, checkpointed MP4 top-level-atom screening. No source or public writes."""
import pathlib,os,json,gzip,sqlite3,datetime,time,struct,collections,hashlib,hmac,urllib.request,urllib.parse,urllib.error,concurrent.futures,fcntl
ROOT=pathlib.Path(os.environ['BEST14_RUN_ROOT']).resolve()
STATUS={1:'structure_present',2:'structural_failure',3:'hold'}
STATUS_ID={v:k for k,v in STATUS.items()}
AUDIT=ROOT/'audit'
def header_range(key,offset,total):
 host=os.environ['R2_ACCOUNT_ID']+'.r2.cloudflarestorage.com';uri='/'+urllib.parse.quote(os.environ['R2_BUCKET'],safe='')+'/'+urllib.parse.quote(key,safe='/');stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');day=stamp[:8];empty=hashlib.sha256(b'').hexdigest();scope=day+'/auto/s3/aws4_request'
 canonical='GET\n'+uri+'\n\nhost:'+host+'\nx-amz-content-sha256:'+empty+'\nx-amz-date:'+stamp+'\n\nhost;x-amz-content-sha256;x-amz-date\n'+empty
 signing=('AWS4'+os.environ['R2_SECRET_ACCESS_KEY']).encode()
 for piece in (day,'auto','s3','aws4_request'):signing=hmac.new(signing,piece.encode(),hashlib.sha256).digest()
 signature=hmac.new(signing,('AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()).encode(),hashlib.sha256).hexdigest()
 last=min(offset+15,total-1)
 headers={'Range':f'bytes={offset}-{last}','x-amz-date':stamp,'x-amz-content-sha256':empty,'Authorization':'AWS4-HMAC-SHA256 Credential='+os.environ['R2_ACCESS_KEY_ID']+'/'+scope+', SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature='+signature}
 try:
  with urllib.request.urlopen(urllib.request.Request('https://'+host+uri,headers=headers,method='GET'),timeout=12) as r:
   if r.status!=206 or r.headers.get('Content-Range')!=f'bytes {offset}-{last}/{total}':raise RuntimeError('range_or_size_differs')
   data=r.read(17)
   if len(data)!=last-offset+1:raise RuntimeError('range_body_differs')
   return data,r.headers.get('ETag','')
 except urllib.error.HTTPError as e:raise RuntimeError('http_'+str(e.code)) from None
 except RuntimeError:raise
 except Exception:raise RuntimeError('range_transport_failure') from None

def screen(r):
 size=r['bytes'];pos=0;seen_moov=False;etag=None;reads=0;deadline=time.monotonic()+24
 try:
  for _ in range(32):
   if time.monotonic()>=deadline:return 'hold','object_screen_deadline',reads
   if pos>=size:return 'structural_failure','no_moov',reads
   reads+=1;raw,e=header_range(r['object_key'],pos,size)
   if etag is not None and e!=etag:return 'hold','object_changed_during_screen',reads
   etag=e
   if len(raw)<8:return 'structural_failure','truncated_atom_header',reads
   length,kind=struct.unpack('>I4s',raw[:8]);header=8
   if length==1:
    if len(raw)<16:return 'structural_failure','truncated_extended_atom_header',reads
    length=struct.unpack('>Q',raw[8:16])[0];header=16
   if length==0:length=size-pos
   if length<header or pos+length>size:return 'structural_failure','atom_outside_object',reads
   if kind==b'moov':return 'structure_present','moov_bounds_present_not_decode_qualified',reads
   pos+=length
  return 'hold','atom_scan_bound',reads
 except RuntimeError as e:return 'hold',str(e),reads

def save_result(db,row,result,line_no,checkpoint=False):
 status,reason,reads=result
 db.execute('INSERT OR REPLACE INTO screened VALUES(?,?,?,?,?,?,?)',(row['clip_id'],row['recording_id'],bytes.fromhex(row['sha256']),STATUS_ID[status],1 if reason=='moov_bounds_present_not_decode_qualified' else reason,reads,time.time()))
 if checkpoint:
  db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('next_line',str(line_no)));db.commit()


def main():
 if (ROOT/'STOP').exists():return
 ledger=ROOT/'budget-ledger.json'
 if ledger.exists() and json.loads(ledger.read_text()).get('upper_bound_usd',0)>=90:return
 lock=open(ROOT/'structure-screen.lock','a+')
 try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError:return
 db=sqlite3.connect(ROOT/'structure-screen.sqlite3');db.execute('PRAGMA journal_mode=DELETE');db.execute('CREATE TABLE IF NOT EXISTS screened(clip_id INTEGER PRIMARY KEY,recording_id INTEGER,sha256 BLOB,status INTEGER,reason,reads INTEGER,observed_at REAL)');db.execute('CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT)')
 meta=dict(db.execute('SELECT key,value FROM meta'))
 if meta.get('complete')=='true':db.close();return
 known={r[0]:(r[1].hex() if isinstance(r[1],bytes) else r[1],STATUS.get(r[2],r[2])) for r in db.execute('SELECT clip_id,sha256,status FROM screened')};next_line=int(meta.get('next_line','0'));public={x['recording_id'] for x in json.loads((AUDIT/'selection-final-v2.json').read_text())['targets'] if x['published_period']};batch=[];batch_lines=[];line_no=next_line;end=False
 with gzip.open(AUDIT/'stage3-v2-missing-sources.jsonl.gz','rt') as stream:
  for i,line in enumerate(stream):
   if i<next_line:continue
   line_no=i+1;r=json.loads(line)
   if r['recording_id'] in public and known.get(r['clip_id']) not in [(r['sha256'],'structure_present'),(r['sha256'],'structural_failure')]:batch.append(r);batch_lines.append(line_no)
   if len(batch)>=1000:break
  else:end=True
 # Positive 5GiB reserve remains after the small checkpoint allocation.
 if os.statvfs(ROOT).f_bavail*os.statvfs(ROOT).f_frsize<5*1024**3+8*1024**2:raise RuntimeError('metadata checkpoint deferred: storage floor')
 started=time.monotonic()
 with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
  for index,(r,result) in enumerate(zip(batch,pool.map(screen,batch)),1):
   save_result(db,r,result,batch_lines[index-1],checkpoint=index%16==0)

 counts=collections.Counter()
 for status,count in db.execute('SELECT status,count(*) FROM screened GROUP BY status'):counts[STATUS.get(status,status)]+=count
 complete=counts.get('structure_present',0)+counts.get('structural_failure',0)==248402;db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('next_line',str(0 if end else line_no)));db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('complete',str(complete).lower()));db.commit();total_reads=db.execute('SELECT coalesce(sum(reads),0) FROM screened').fetchone()[0];db.close()
 result={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'next_line':line_no,'complete':complete,'expected_public_sources':248402,'new_batch':len(batch),'seconds':round(time.monotonic()-started,2),'counts':counts,'range_requests':total_reads,'publication_changed':False,'source_payload_writes':False};temporary=ROOT/'structure-screen-status.json.pending';temporary.write_text(json.dumps(result,indent=2));os.replace(temporary,ROOT/'structure-screen-status.json');print(json.dumps(result),flush=True)
if __name__=='__main__':main()
